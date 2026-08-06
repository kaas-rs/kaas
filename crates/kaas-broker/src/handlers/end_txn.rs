//! EndTxn handler (key 26, v0–v3).
//!
//! Three steps, in this order, and the order is the correctness
//! property (gh #225):
//!
//! 1. Validate ownership + (pid, epoch), then **prepare** the txn via
//!    [`TxnStateStore::prepare_end_txn`] — `Ongoing → Prepare{Commit,
//!    Abort}`, keeping the partition list on the persisted entry.
//! 2. Place a COMMIT / ABORT marker for every partition in that list.
//!    Partitions this broker leads get a [`build_control_batch`]
//!    control batch appended with `acks = -1`; peer-led partitions get
//!    a marker-queue entry on the shared PVC, which the peer's
//!    `MarkerWatcher` retries until applied (gh #175).
//! 3. Only once every marker is durable, **complete** via
//!    [`TxnStateStore::complete_end_txn`] — `Prepare* → Complete*`,
//!    clearing the list and firing the [`TxnOffsetHook`] that
//!    materialises or discards staged offsets.
//!
//! If step 2 fails for any partition the handler returns a retriable
//! error and the entry stays in `Prepare*` with its list intact, so the
//! producer's retry re-drives the identical dispatch set. Before
//! gh #225 the state advanced to `Complete*` first, the list was
//! cleared, dispatch failures were logged and swallowed, and EndTxn
//! answered success regardless — leaving `read_committed` consumers
//! with a permanently pinned LSO and nothing able to notice.
//!
//! `acks = -1` for local marker dispatch: control markers commit
//! transactions, so they must be durable before we ack the producer.
//!
//! [`TxnStateStore::prepare_end_txn`]: kaas_coordinator::TxnStateStore::prepare_end_txn
//! [`TxnStateStore::complete_end_txn`]: kaas_coordinator::TxnStateStore::complete_end_txn
//! [`TxnOffsetHook`]: kaas_coordinator::TxnOffsetHook
//! [`build_control_batch`]: crate::control_batch::build_control_batch

use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use kaas_codec::api::end_txn;
use kaas_coordinator::{EndTxnOutcome, MarkerEntry, TxnStateError, TxnTopic};
use kaas_protocol::{ConnState, Handler, HandlerError};
use parking_lot::Mutex;

use std::collections::HashMap;

use crate::broker::Broker;
use crate::control_batch::build_control_batch;

const ERR_INVALID_REQUEST: i16 = 42;
const ERR_NOT_COORDINATOR: i16 = 16;
const ERR_COORDINATOR_NOT_AVAILABLE: i16 = 15;
const ERR_INVALID_PRODUCER_ID_MAPPING: i16 = 49;
const ERR_PRODUCER_FENCED: i16 = 90;
const ERR_CONCURRENT_TRANSACTIONS: i16 = 51;
const ERR_INVALID_TXN_STATE: i16 = 50;
const ACKS_ALL: i16 = -1;

#[derive(Debug)]
pub struct EndTxnHandler {
    broker: Arc<Broker>,
}

impl EndTxnHandler {
    pub fn new(broker: Arc<Broker>) -> Self {
        Self { broker }
    }
}

#[async_trait]
impl Handler for EndTxnHandler {
    async fn handle(
        &self,
        _conn: &Mutex<ConnState>,
        version: i16,
        body: Bytes,
    ) -> Result<BytesMut, HandlerError> {
        let mut body = body;
        let req = end_txn::decode_request(&mut body, version)?;

        let error_code = match self.classify(&req) {
            Some(code) => code,
            None => self.transition_and_dispatch(&req).await,
        };

        let resp = end_txn::Response {
            throttle_time_ms: 0,
            error_code,
        };
        let mut out = BytesMut::new();
        end_txn::encode_response(&mut out, &resp, version)?;
        Ok(out)
    }
}

impl EndTxnHandler {
    fn classify(&self, req: &end_txn::Request) -> Option<i16> {
        if req.transactional_id.is_empty() {
            return Some(ERR_INVALID_REQUEST);
        }
        if !self.broker.owns_txn(&req.transactional_id) {
            return Some(ERR_NOT_COORDINATOR);
        }
        if self.broker.txn_state().is_none() {
            return Some(ERR_COORDINATOR_NOT_AVAILABLE);
        }
        None
    }

    /// Two-phase EndTxn (gh #225).
    ///
    /// ```text
    /// prepare  →  dispatch every marker  →  complete
    ///                     │
    ///                     └─ any failure: stay in Prepare*, return a
    ///                        retriable code, producer retry re-drives
    /// ```
    ///
    /// The ordering is the fix. Before gh #225 the store transitioned
    /// straight to `Complete*` — clearing the partition list — and then
    /// dispatched markers on a best-effort basis, logging and swallowing
    /// every failure while still answering `error_code = 0`. A marker
    /// that failed to land was then unrecoverable: the dispatch set was
    /// gone, a producer retry was a documented no-op, and no reconcile
    /// pass existed. For `read_committed` consumers that pins the
    /// partition's LSO forever — not just for the lost transaction, but
    /// for every committed record written after it.
    ///
    /// So: never complete before the markers are durable, and never
    /// answer success for a transaction whose markers we could not
    /// place.
    async fn transition_and_dispatch(&self, req: &end_txn::Request) -> i16 {
        let store = match self.broker.txn_state() {
            Some(s) => s,
            None => return ERR_COORDINATOR_NOT_AVAILABLE,
        };
        let outcome = match store.prepare_end_txn(
            &req.transactional_id,
            req.producer_id,
            req.producer_epoch,
            req.committed,
        ) {
            Ok(o) => o,
            Err(e) => return map_store_error(&e),
        };
        // Already `Complete*` — markers were written on an earlier
        // attempt, nothing owed.
        if !outcome.transition_fired {
            return 0;
        }
        if let Err(code) = self.dispatch_markers(req, &outcome).await {
            // Entry stays in Prepare* with its partition list intact.
            // The producer's retry re-drives the identical dispatch set.
            tracing::warn!(
                txn_id = %req.transactional_id,
                producer_id = req.producer_id,
                error_code = code,
                "EndTxn: marker dispatch incomplete; transaction left in Prepare \
                 state for retry rather than acked as complete (gh #225)",
            );
            return code;
        }
        match store.complete_end_txn(
            &req.transactional_id,
            req.producer_id,
            req.producer_epoch,
            req.committed,
        ) {
            Ok(()) => 0,
            Err(e) => map_store_error(&e),
        }
    }

    /// Place a COMMIT/ABORT marker for every partition in the dispatch
    /// set. `Ok(())` means every marker is durable: locally appended
    /// with `acks = -1`, or written to the peer's marker-queue inbox on
    /// the shared PVC (from where the peer's `MarkerWatcher` retries
    /// until it applies). `Err(code)` is a retriable wire code.
    async fn dispatch_markers(
        &self,
        req: &end_txn::Request,
        outcome: &EndTxnOutcome,
    ) -> Result<(), i16> {
        if outcome.partitions.is_empty() {
            return Ok(());
        }

        // Group partitions by which broker leads them. Same-broker
        // partitions are written locally (low latency); peer-broker
        // partitions go through the marker_queue (gh #175 file-queue
        // dispatch). Coordinator-less dev mode treats every partition
        // as same-broker.
        let mut by_target: HashMap<Option<String>, Vec<(String, i32)>> = HashMap::new();
        let coord = self.broker.coordinator();
        for TxnTopic { topic, partitions } in &outcome.partitions {
            for &p in partitions {
                let leader = coord.as_ref().and_then(|c| c.leader_for(topic, p));
                by_target
                    .entry(leader)
                    .or_default()
                    .push((topic.clone(), p));
            }
        }

        let self_id = self.broker.self_id.as_str();
        // Splits: (local writes, per-target queue entries).
        let mut local_partitions: Vec<(String, i32)> = Vec::new();
        let mut queued: HashMap<String, Vec<(String, i32)>> = HashMap::new();
        for (target, parts) in by_target {
            match target {
                None => local_partitions.extend(parts), // dev mode
                Some(id) if id == self_id => local_partitions.extend(parts),
                Some(id) => {
                    queued.entry(id).or_default().extend(parts);
                }
            }
        }

        // Same-broker write — happens before the queue write so a
        // crash mid-dispatch still leaves the local marker in place.
        if !local_partitions.is_empty() {
            self.write_local_markers(req, &local_partitions).await?;
        }

        // Cross-broker dispatch via the shared-PVC queue. Receiver's
        // MarkerWatcher picks it up within ~2 s and applies it on the
        // peer leader (gh #175).
        if !queued.is_empty() {
            self.enqueue_cross_broker_markers(req, &queued)?;
        }
        Ok(())
    }

    /// Append the control batch to every locally-led partition.
    ///
    /// A failed append aborts the whole dispatch: the caller leaves the
    /// txn in Prepare* so the retry re-appends. Re-appending a marker
    /// the first attempt already landed writes a duplicate control
    /// batch, which is wasteful but not incorrect — control batches
    /// carry no sequence numbers and consumers don't react to duplicate
    /// markers (same property `MarkerWatcher` already relies on).
    async fn write_local_markers(
        &self,
        req: &end_txn::Request,
        partitions: &[(String, i32)],
    ) -> Result<(), i16> {
        let batch = Bytes::from(build_control_batch(
            req.producer_id,
            req.producer_epoch,
            req.committed,
            // CoordinatorEpoch — Apache populates it from the txn
            // coordinator's lease epoch. Phase 6 doesn't track that
            // distinctly from the assignment epoch; 0 keeps the wire
            // shape valid (consumers don't act on the field).
            0,
        ));
        for (topic, p) in partitions {
            let epoch = self
                .broker
                .coordinator()
                .and_then(|c| c.current_epoch(topic, *p))
                .unwrap_or_else(|| self.broker.local_lease.current_epoch());
            let _ = self.broker.engine.create_partition(topic, *p).await;
            if let Err(err) = self
                .broker
                .engine
                .append(topic, *p, epoch, ACKS_ALL, batch.clone())
                .await
            {
                tracing::warn!(
                    topic,
                    partition = p,
                    %err,
                    "EndTxn marker append failed; transaction stays in Prepare state \
                     and the producer's retry re-drives this marker (gh #225)",
                );
                return Err(ERR_COORDINATOR_NOT_AVAILABLE);
            }
        }
        Ok(())
    }

    /// Hand peer-led partitions to the shared-PVC marker queue.
    ///
    /// A successful `enqueue` is the durability boundary for a peer
    /// partition: the entry is on the shared volume, and the peer's
    /// `MarkerWatcher` retries application until it succeeds
    /// (`ApplyOutcome::Retry` keeps the file). So enqueue success is
    /// enough to complete the transaction; only enqueue *failure* is
    /// fatal to this attempt.
    fn enqueue_cross_broker_markers(
        &self,
        req: &end_txn::Request,
        queued: &HashMap<String, Vec<(String, i32)>>,
    ) -> Result<(), i16> {
        let queue = match self.broker.marker_queue() {
            Some(q) => q,
            None => {
                // Previously this returned silently and EndTxn still
                // answered success, so peer partitions kept their LSO
                // pinned with nothing left to notice it (gh #225).
                tracing::warn!(
                    txn_id = %req.transactional_id,
                    "EndTxn: cross-broker markers needed but no MarkerQueue is wired; \
                     refusing to complete the transaction",
                );
                return Err(ERR_COORDINATOR_NOT_AVAILABLE);
            }
        };
        for (target_broker, parts) in queued {
            // Pack into TxnTopic so the schema matches what
            // MarkerWatcher applies on the other side.
            let mut by_topic: HashMap<String, Vec<i32>> = HashMap::new();
            for (topic, p) in parts {
                by_topic.entry(topic.clone()).or_default().push(*p);
            }
            let partitions: Vec<TxnTopic> = by_topic
                .into_iter()
                .map(|(topic, partitions)| TxnTopic { topic, partitions })
                .collect();
            let entry = MarkerEntry {
                transactional_id: req.transactional_id.clone(),
                producer_id: req.producer_id,
                producer_epoch: req.producer_epoch,
                commit: req.committed,
                coordinator_epoch: 0,
                partitions,
            };
            if let Err(err) = queue.enqueue(target_broker, &entry) {
                tracing::warn!(
                    target = %target_broker,
                    txn_id = %req.transactional_id,
                    %err,
                    "EndTxn: marker queue enqueue failed; transaction stays in \
                     Prepare state for the producer's retry (gh #225)"
                );
                return Err(ERR_COORDINATOR_NOT_AVAILABLE);
            }
        }
        Ok(())
    }
}

fn map_store_error(err: &TxnStateError) -> i16 {
    match err {
        TxnStateError::EmptyTxnId => ERR_INVALID_REQUEST,
        TxnStateError::UnknownProducer => ERR_INVALID_PRODUCER_ID_MAPPING,
        TxnStateError::EpochFenced => ERR_PRODUCER_FENCED,
        TxnStateError::Concurrent => ERR_CONCURRENT_TRANSACTIONS,
        TxnStateError::InvalidState => ERR_INVALID_TXN_STATE,
        TxnStateError::Io(_) | TxnStateError::Decode(_) => ERR_COORDINATOR_NOT_AVAILABLE,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::topic_registry::TopicRegistry;
    use kaas_coordinator::{TxnStateStore, TxnTopic};
    use kaas_storage::{MemoryStorage, StorageEngine};
    use std::net::SocketAddr;
    use std::str::FromStr;

    fn conn() -> Mutex<ConnState> {
        Mutex::new(ConnState::new(
            "internal",
            SocketAddr::from_str("127.0.0.1:9092").unwrap(),
        ))
    }

    fn broker_with_txn() -> (tempfile::TempDir, Arc<Broker>) {
        let tmp = tempfile::tempdir().unwrap();
        let engine: Arc<dyn StorageEngine> = Arc::new(MemoryStorage::new());
        let b = Arc::new(Broker::new(
            engine,
            Arc::new(TopicRegistry::new()),
            "test",
            0,
        ));
        b.install_txn_state(Arc::new(TxnStateStore::open(tmp.path(), 0).unwrap()));
        (tmp, b)
    }

    fn encode_request(req: &end_txn::Request, version: i16) -> Bytes {
        use kaas_codec::api::common::write_str;
        use kaas_codec::primitives::{write_i16, write_i64, write_i8};
        use kaas_codec::tagged;
        let flexible = version >= end_txn::MIN_FLEXIBLE;
        let mut w = BytesMut::new();
        write_str(&mut w, &req.transactional_id, flexible).unwrap();
        write_i64(&mut w, req.producer_id);
        write_i16(&mut w, req.producer_epoch);
        write_i8(&mut w, if req.committed { 1 } else { 0 });
        if flexible {
            tagged::write_empty(&mut w);
        }
        w.freeze()
    }

    async fn call(h: &EndTxnHandler, req: &end_txn::Request) -> end_txn::Response {
        let body = encode_request(req, 3);
        let out = h.handle(&conn(), 3, body).await.unwrap();
        let mut r = out.freeze();
        end_txn::decode_response(&mut r, 3).unwrap()
    }

    #[tokio::test]
    async fn commit_happy_path_writes_marker_to_owned_partition() {
        let (_t, b) = broker_with_txn();
        let store = b.txn_state().unwrap();
        let (pid, epoch) = store.get_or_allocate("tx-1", || 1).unwrap();
        store
            .add_partitions(
                "tx-1",
                pid,
                epoch,
                &[TxnTopic {
                    topic: "t".into(),
                    partitions: vec![0],
                }],
                100,
            )
            .unwrap();

        // Pre-create the partition so the marker append has somewhere
        // to land.
        b.engine.create_partition("t", 0).await.unwrap();
        let hwm_before = b.engine.high_watermark("t", 0).unwrap();

        let h = EndTxnHandler::new(b.clone());
        let resp = call(
            &h,
            &end_txn::Request {
                transactional_id: "tx-1".into(),
                producer_id: pid,
                producer_epoch: epoch,
                committed: true,
            },
        )
        .await;
        assert_eq!(resp.error_code, 0);

        // State was cleared and a marker batch appended (HWM advanced).
        let snap = store.snapshot();
        assert!(snap["tx-1"].partitions.is_empty());
        let hwm_after = b.engine.high_watermark("t", 0).unwrap();
        assert!(
            hwm_after > hwm_before,
            "expected HWM to advance after marker append; before={hwm_before} after={hwm_after}"
        );
    }

    #[tokio::test]
    async fn end_txn_against_empty_returns_invalid_txn_state() {
        let (_t, b) = broker_with_txn();
        let store = b.txn_state().unwrap();
        let (pid, epoch) = store.get_or_allocate("tx-1", || 1).unwrap();
        let h = EndTxnHandler::new(b);
        let resp = call(
            &h,
            &end_txn::Request {
                transactional_id: "tx-1".into(),
                producer_id: pid,
                producer_epoch: epoch,
                committed: true,
            },
        )
        .await;
        assert_eq!(resp.error_code, ERR_INVALID_TXN_STATE);
    }

    #[tokio::test]
    async fn epoch_mismatch_returns_producer_fenced() {
        let (_t, b) = broker_with_txn();
        let store = b.txn_state().unwrap();
        let (pid, _epoch) = store.get_or_allocate("tx-1", || 1).unwrap();
        let h = EndTxnHandler::new(b);
        let resp = call(
            &h,
            &end_txn::Request {
                transactional_id: "tx-1".into(),
                producer_id: pid,
                producer_epoch: 99,
                committed: true,
            },
        )
        .await;
        assert_eq!(resp.error_code, ERR_PRODUCER_FENCED);
    }

    /// Build a coordinator that says `t/0` is led by a *peer*, so the
    /// dispatch takes the cross-broker (marker-queue) path rather than
    /// the local-append one.
    fn install_peer_leader_coordinator(b: &Arc<Broker>, cluster_dir: &std::path::Path) {
        use crate::assignment::{
            Assignment, BrokerAssignment, BrokerHealth, PartitionAssignment, PartitionRole,
        };
        use crate::coordinator::{Coordinator, LocalHeartbeat, LocalLeaseEpoch};
        let a = Assignment {
            controller_epoch: 1,
            assignment_version: 1,
            generated_at: "2026-01-01T00:00:00Z".to_owned(),
            controller: "test".to_owned(),
            brokers: vec![
                BrokerAssignment {
                    id: "test".to_owned(),
                    health: BrokerHealth::Alive,
                    last_seen: "x".to_owned(),
                },
                BrokerAssignment {
                    id: "kaas-1".to_owned(),
                    health: BrokerHealth::Alive,
                    last_seen: "x".to_owned(),
                },
            ],
            partitions: vec![PartitionAssignment {
                topic: "t".to_owned(),
                partition: 0,
                broker: "kaas-1".to_owned(), // peer leads it
                epoch: 1,
                role: PartitionRole::Leader,
            }],
            consumer_groups: Vec::new(),
        };
        std::fs::write(
            cluster_dir.join("assignment.json"),
            serde_json::to_vec(&a).unwrap(),
        )
        .unwrap();
        let c = Coordinator::new(
            "test",
            cluster_dir,
            Arc::new(LocalLeaseEpoch),
            Arc::new(LocalHeartbeat),
        );
        assert!(c.apply_if_new(), "coordinator must load the assignment");
        assert_eq!(c.leader_for("t", 0).as_deref(), Some("kaas-1"));
        b.install_coordinator(c);
    }

    fn ongoing_txn(b: &Arc<Broker>) -> (i64, i16) {
        let store = b.txn_state().unwrap();
        let (pid, epoch) = store.get_or_allocate("tx-1", || 1).unwrap();
        store
            .add_partitions(
                "tx-1",
                pid,
                epoch,
                &[TxnTopic {
                    topic: "t".into(),
                    partitions: vec![0],
                }],
                100,
            )
            .unwrap();
        (pid, epoch)
    }

    /// gh #225: a transaction whose markers could not be placed must
    /// NOT be acked as committed, and must stay re-drivable.
    ///
    /// Cross-broker partition + no MarkerQueue wired = nowhere to put
    /// the marker. The old code logged, returned early, and answered
    /// `error_code = 0` with the partition list already cleared, so the
    /// peer's LSO stayed pinned forever with nothing left to notice.
    #[tokio::test]
    async fn dispatch_failure_is_not_acked_as_success_and_stays_recoverable() {
        let (t, b) = broker_with_txn();
        install_peer_leader_coordinator(&b, t.path());
        let (pid, epoch) = ongoing_txn(&b);

        let h = EndTxnHandler::new(b.clone());
        let resp = call(
            &h,
            &end_txn::Request {
                transactional_id: "tx-1".into(),
                producer_id: pid,
                producer_epoch: epoch,
                committed: true,
            },
        )
        .await;

        assert_ne!(
            resp.error_code, 0,
            "EndTxn must not ack success when no marker could be placed"
        );
        assert_eq!(resp.error_code, ERR_COORDINATOR_NOT_AVAILABLE);

        // The dispatch set survives for the retry.
        let snap = b.txn_state().unwrap().snapshot();
        assert_eq!(
            snap["tx-1"].state,
            kaas_coordinator::TxnState::PrepareCommit
        );
        assert!(
            !snap["tx-1"].partitions.is_empty(),
            "partition list must be retained so a retry re-drives the same markers"
        );
    }

    /// The other half: once dispatch can succeed, the producer's retry
    /// pushes the same transaction through to Complete.
    #[tokio::test]
    async fn retry_after_dispatch_failure_completes_and_enqueues_the_marker() {
        let (t, b) = broker_with_txn();
        install_peer_leader_coordinator(&b, t.path());
        let (pid, epoch) = ongoing_txn(&b);
        let h = EndTxnHandler::new(b.clone());
        let req = end_txn::Request {
            transactional_id: "tx-1".into(),
            producer_id: pid,
            producer_epoch: epoch,
            committed: true,
        };

        // First attempt fails — no queue wired.
        assert_ne!(call(&h, &req).await.error_code, 0);

        // Operator/bootstrap wires the queue; producer retries.
        let queue = kaas_coordinator::MarkerQueue::open(t.path()).unwrap();
        b.install_marker_queue(queue.clone());
        let resp = call(&h, &req).await;
        assert_eq!(resp.error_code, 0, "retry must push the txn through");

        let pending = queue.list("kaas-1").unwrap();
        assert_eq!(pending.len(), 1, "peer marker must be queued");
        assert_eq!(pending[0].1.producer_id, pid);
        assert!(pending[0].1.commit);

        let snap = b.txn_state().unwrap().snapshot();
        assert_eq!(
            snap["tx-1"].state,
            kaas_coordinator::TxnState::CompleteCommit
        );
        assert!(snap["tx-1"].partitions.is_empty());
    }

    #[tokio::test]
    async fn idempotent_retry_after_commit_is_noop() {
        let (_t, b) = broker_with_txn();
        let store = b.txn_state().unwrap();
        let (pid, epoch) = store.get_or_allocate("tx-1", || 1).unwrap();
        store
            .add_partitions(
                "tx-1",
                pid,
                epoch,
                &[TxnTopic {
                    topic: "t".into(),
                    partitions: vec![0],
                }],
                100,
            )
            .unwrap();
        b.engine.create_partition("t", 0).await.unwrap();
        let h = EndTxnHandler::new(b.clone());
        call(
            &h,
            &end_txn::Request {
                transactional_id: "tx-1".into(),
                producer_id: pid,
                producer_epoch: epoch,
                committed: true,
            },
        )
        .await;
        let hwm_after_first = b.engine.high_watermark("t", 0).unwrap();
        // Retry — should be Ok with no extra marker write.
        let resp = call(
            &h,
            &end_txn::Request {
                transactional_id: "tx-1".into(),
                producer_id: pid,
                producer_epoch: epoch,
                committed: true,
            },
        )
        .await;
        assert_eq!(resp.error_code, 0);
        let hwm_after_retry = b.engine.high_watermark("t", 0).unwrap();
        assert_eq!(
            hwm_after_first, hwm_after_retry,
            "idempotent retry must not write a second marker"
        );
    }
}
