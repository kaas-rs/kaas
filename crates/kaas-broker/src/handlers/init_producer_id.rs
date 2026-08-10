//! InitProducerId handler (key 22).
//!
//! InitProducerId (API key 22) with the transactional path wired in.
//!
//! Non-transactional (`transactional_id: None`) — hand out a fresh
//! PID with `epoch = 0` from the broker's monotonic counter. Same as
//! Phase 3.
//!
//! Transactional (`transactional_id: Some(_)`):
//! 0. gh #199 ACL gate — `Write` on the `TransactionalId` resource,
//!    else `TRANSACTIONAL_ID_AUTHORIZATION_FAILED` (53). Checked
//!    before routing so an unauthorized client learns nothing about
//!    coordinator placement.
//! 1. gh #91 routing gate — if [`Broker::owns_txn`] says no, return
//!    `NOT_COORDINATOR` (16) so the Java client `markCoordinatorUnknown`
//!    + re-FindCoordinator path lands on the right broker.
//! 2. gh #22 rejoin contract — if a [`TxnStateStore`] is installed,
//!    call `get_or_allocate_with_timeout`. First call returns a
//!    fresh PID with `epoch = 0`; every subsequent call returns the
//!    SAME PID with `epoch += 1`.
//! 3. Without a store (boot window / dev mode), fall back to fresh
//!    PID + a one-shot `tracing::warn!` (graceful degradation). A zombie writer could still squeak under
//!    its old (PID, epoch) in that window.
//!
//! Cross-broker fence broadcast (gh #30 / #108 phase 2) lands in
//! workstream E.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use kaas_auth::{Operation, Resource};
use kaas_codec::api::init_producer_id;
use kaas_coordinator::TxnStateError;
use kaas_protocol::{ConnState, Handler, HandlerError};
use parking_lot::Mutex;

use super::principal_from;
use crate::broker::Broker;

/// `NOT_COORDINATOR` — gh #91 routing miss.
pub const ERR_NOT_COORDINATOR: i16 = 16;
/// `TRANSACTIONAL_ID_AUTHORIZATION_FAILED` — gh #199 ACL gate.
pub const ERR_TXN_ID_AUTHZ_FAILED: i16 = 53;
/// `COORDINATOR_NOT_AVAILABLE` — txn store not yet wired.
pub const ERR_COORDINATOR_NOT_AVAILABLE: i16 = 15;
/// `PRODUCER_FENCED` — epoch mismatch.
pub const ERR_PRODUCER_FENCED: i16 = 90;
/// `INVALID_TXN_STATE` — defensive; unreachable for InitProducerId
/// today but kept in the mapping for forward compat.
pub const ERR_INVALID_TXN_STATE: i16 = 50;

#[derive(Debug)]
pub struct InitProducerIdHandler {
    broker: Arc<Broker>,
}

impl InitProducerIdHandler {
    pub fn new(broker: Arc<Broker>) -> Self {
        Self { broker }
    }
}

#[async_trait]
impl Handler for InitProducerIdHandler {
    async fn handle(
        &self,
        conn: &Mutex<ConnState>,
        version: i16,
        body: Bytes,
    ) -> Result<BytesMut, HandlerError> {
        let mut body = body;
        let req = init_producer_id::decode_request(&mut body, version)?;

        let resp = match req.transactional_id.as_deref() {
            None | Some("") => {
                // Idempotent (non-transactional) — no per-key state,
                // every broker can answer locally. Deliberately NOT
                // gated on IDEMPOTENT_WRITE: the Java client enables
                // idempotence by default since 3.0, and Apache itself
                // relaxed the gate (KIP-679) so a topic-WRITE-only
                // principal can still produce. The real enforcement
                // point is Produce's per-topic Write check.
                init_producer_id::Response {
                    throttle_time_ms: 0,
                    error_code: 0,
                    producer_id: self.broker.next_producer_id(),
                    producer_epoch: 0,
                }
            }
            Some(txn_id) => {
                // gh #199: Apache gates the transactional path on
                // WRITE for the TransactionalId resource, before any
                // coordinator routing is revealed.
                let principal = principal_from(conn);
                if !self.broker.authorizer.authorize(
                    &principal,
                    &Resource::transactional_id(txn_id),
                    Operation::Write,
                ) {
                    error_response(ERR_TXN_ID_AUTHZ_FAILED)
                } else {
                    self.handle_transactional(txn_id, req.transaction_timeout_ms)
                }
            }
        };

        let mut out = BytesMut::new();
        init_producer_id::encode_response(&mut out, &resp, version)?;
        Ok(out)
    }
}

impl InitProducerIdHandler {
    fn handle_transactional(&self, txn_id: &str, timeout_ms: i32) -> init_producer_id::Response {
        // gh #91 routing — reject on the wrong broker before
        // touching the PID counter or the store.
        if !self.broker.owns_txn(txn_id) {
            return error_response(ERR_NOT_COORDINATOR);
        }
        let store = match self.broker.txn_state() {
            Some(s) => s,
            None => {
                // No store wired (boot window or dev mode):
                // hand out a fresh PID, log once.
                // The producer can make progress but the gh #22
                // fence-on-rejoin contract is silently disabled
                // this connection.
                tracing::warn!(
                    txn_id = %txn_id,
                    "InitProducerId received transactional.id but no TxnStateStore is wired; \
                     epoch fence on rejoin disabled (gh #22)",
                );
                return init_producer_id::Response {
                    throttle_time_ms: 0,
                    error_code: 0,
                    producer_id: self.broker.next_producer_id(),
                    producer_epoch: 0,
                };
            }
        };
        let broker_for_alloc = self.broker.clone();
        match store.get_or_allocate_with_timeout(txn_id, timeout_ms, move || {
            broker_for_alloc.next_producer_id()
        }) {
            Ok((pid, epoch)) => {
                // gh #30: on every epoch bump, fence locally + broadcast
                // to peers via the outbound FenceLog so their
                // FenceWatcher applies it within ~2s. `epoch == 0`
                // covers two cases — first-ever alloc, and post-
                // overflow rotation to a fresh PID — neither needs
                // fencing (no earlier (pid, epoch) state exists, or
                // the PID itself has changed).
                if epoch > 0 {
                    self.broker.engine.fence_producer_epoch(pid, epoch);
                    if let Some(log) = self.broker.fence_log() {
                        if let Err(err) = log.append(pid, epoch) {
                            tracing::warn!(
                                pid,
                                epoch,
                                %err,
                                "InitProducerId: outbound FenceLog append failed; \
                                 peer brokers will not see this epoch bump until \
                                 a future bump succeeds (zombie window on cross-broker partitions)",
                            );
                        }
                    }
                }
                init_producer_id::Response {
                    throttle_time_ms: 0,
                    error_code: 0,
                    producer_id: pid,
                    producer_epoch: epoch,
                }
            }
            Err(err) => error_response(map_store_error(&err)),
        }
    }
}

fn error_response(code: i16) -> init_producer_id::Response {
    init_producer_id::Response {
        throttle_time_ms: 0,
        error_code: code,
        producer_id: -1,
        producer_epoch: -1,
    }
}

fn map_store_error(err: &TxnStateError) -> i16 {
    match err {
        TxnStateError::EmptyTxnId => 42,      // INVALID_REQUEST
        TxnStateError::UnknownProducer => 49, // INVALID_PRODUCER_ID_MAPPING
        TxnStateError::EpochFenced => ERR_PRODUCER_FENCED,
        TxnStateError::Concurrent => 51, // CONCURRENT_TRANSACTIONS
        TxnStateError::InvalidState => ERR_INVALID_TXN_STATE,
        // Persistence failure — surface as COORDINATOR_NOT_AVAILABLE so the
        // client retries instead of silently bypassing the fence.
        TxnStateError::Io(_) | TxnStateError::Decode(_) => ERR_COORDINATOR_NOT_AVAILABLE,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::topic_registry::TopicRegistry;
    use kaas_coordinator::TxnStateStore;
    use kaas_storage::{MemoryStorage, StorageEngine};
    use std::net::SocketAddr;
    use std::str::FromStr;

    fn conn() -> Mutex<ConnState> {
        Mutex::new(ConnState::new(
            "internal",
            SocketAddr::from_str("127.0.0.1:9092").unwrap(),
        ))
    }

    fn broker() -> Arc<Broker> {
        let engine: Arc<dyn StorageEngine> = Arc::new(MemoryStorage::new());
        Arc::new(Broker::new(
            engine,
            Arc::new(TopicRegistry::new()),
            "test",
            0,
        ))
    }

    fn broker_with_txn() -> (tempfile::TempDir, Arc<Broker>) {
        let tmp = tempfile::tempdir().unwrap();
        let b = broker();
        let store = Arc::new(TxnStateStore::open(tmp.path(), 0).unwrap());
        b.install_txn_state(store);
        (tmp, b)
    }

    fn encode_request(req: &init_producer_id::Request, version: i16) -> Bytes {
        use kaas_codec::api::common::write_nullable_str;
        use kaas_codec::primitives::{write_i16, write_i32, write_i64};
        use kaas_codec::tagged;
        let flexible = version >= init_producer_id::MIN_FLEXIBLE;
        let mut w = BytesMut::new();
        write_nullable_str(&mut w, req.transactional_id.as_deref(), flexible).unwrap();
        write_i32(&mut w, req.transaction_timeout_ms);
        if version >= 3 {
            write_i64(&mut w, req.producer_id);
            write_i16(&mut w, req.producer_epoch);
        }
        if flexible {
            tagged::write_empty(&mut w);
        }
        w.freeze()
    }

    async fn call(
        h: &InitProducerIdHandler,
        txn_id: Option<&str>,
        timeout_ms: i32,
    ) -> init_producer_id::Response {
        let req = init_producer_id::Request {
            transactional_id: txn_id.map(str::to_owned),
            transaction_timeout_ms: timeout_ms,
            producer_id: -1,
            producer_epoch: -1,
        };
        let body = encode_request(&req, 4);
        let out = h.handle(&conn(), 4, body).await.unwrap();
        let mut r = out.freeze();
        init_producer_id::decode_response(&mut r, 4).unwrap()
    }

    #[tokio::test]
    async fn transactional_denied_returns_txn_id_authz_failed() {
        // gh #199: `Write` on the TransactionalId resource gates the
        // transactional path, before any coordinator routing.
        use crate::handlers::test_authz::DenyAllAuthorizer;
        use kaas_auth::NoQuotaChecker;
        let engine: Arc<dyn kaas_storage::StorageEngine> = Arc::new(MemoryStorage::new());
        let b = Arc::new(Broker::with_auth(
            engine,
            Arc::new(TopicRegistry::new()),
            "test",
            0,
            Arc::new(DenyAllAuthorizer),
            Arc::new(NoQuotaChecker),
        ));
        let h = InitProducerIdHandler::new(b);
        let resp = call(&h, Some("tx-1"), 60_000).await;
        assert_eq!(resp.error_code, ERR_TXN_ID_AUTHZ_FAILED);
        assert_eq!(resp.producer_id, -1);
    }

    #[tokio::test]
    async fn idempotent_path_is_not_gated() {
        // Deliberate (KIP-679): the Java client turns idempotence on
        // by default, so the idempotent InitProducerId stays open even
        // under a deny-all authorizer — Produce's per-topic Write
        // check is the enforcement point.
        use crate::handlers::test_authz::DenyAllAuthorizer;
        use kaas_auth::NoQuotaChecker;
        let engine: Arc<dyn kaas_storage::StorageEngine> = Arc::new(MemoryStorage::new());
        let b = Arc::new(Broker::with_auth(
            engine,
            Arc::new(TopicRegistry::new()),
            "test",
            0,
            Arc::new(DenyAllAuthorizer),
            Arc::new(NoQuotaChecker),
        ));
        let h = InitProducerIdHandler::new(b);
        let resp = call(&h, None, 0).await;
        assert_eq!(resp.error_code, 0);
        assert!(resp.producer_id >= 1);
    }

    #[tokio::test]
    async fn non_transactional_returns_fresh_pid() {
        let h = InitProducerIdHandler::new(broker());
        let resp = call(&h, None, 0).await;
        assert_eq!(resp.error_code, 0);
        assert!(resp.producer_id >= 1);
        assert_eq!(resp.producer_epoch, 0);
    }

    #[tokio::test]
    async fn transactional_without_store_falls_back_to_fresh_pid() {
        // Broker without txn_state installed: fallback —
        // fresh PID + warn, error_code 0.
        let h = InitProducerIdHandler::new(broker());
        let resp = call(&h, Some("tx-1"), 60_000).await;
        assert_eq!(resp.error_code, 0);
        assert!(resp.producer_id >= 1);
        assert_eq!(resp.producer_epoch, 0);
    }

    #[tokio::test]
    async fn transactional_rejoin_bumps_epoch() {
        let (_t, b) = broker_with_txn();
        let h = InitProducerIdHandler::new(b);
        let r1 = call(&h, Some("tx-1"), 60_000).await;
        assert_eq!(r1.error_code, 0);
        assert_eq!(r1.producer_epoch, 0);
        let pid = r1.producer_id;
        let r2 = call(&h, Some("tx-1"), 60_000).await;
        assert_eq!(r2.error_code, 0);
        assert_eq!(r2.producer_id, pid, "rejoin must return same PID");
        assert_eq!(r2.producer_epoch, 1);
        let r3 = call(&h, Some("tx-1"), 60_000).await;
        assert_eq!(r3.producer_epoch, 2);
    }

    #[tokio::test]
    async fn distinct_txn_ids_get_distinct_pids() {
        let (_t, b) = broker_with_txn();
        let h = InitProducerIdHandler::new(b);
        let a = call(&h, Some("tx-a"), 60_000).await;
        let bb = call(&h, Some("tx-b"), 60_000).await;
        assert_ne!(a.producer_id, bb.producer_id);
    }

    #[tokio::test]
    async fn rejoin_appends_to_fence_log_for_broadcast() {
        use kaas_coordinator::FenceLog;
        let (_t, b) = broker_with_txn();
        let fence_dir = tempfile::tempdir().unwrap();
        let log = Arc::new(FenceLog::open(fence_dir.path(), "kaas-0").unwrap());
        b.install_fence_log(log.clone());

        let h = InitProducerIdHandler::new(b);
        let r1 = call(&h, Some("tx-1"), 60_000).await;
        // First call: epoch=0 → no fence broadcast.
        assert_eq!(r1.producer_epoch, 0);
        assert!(log.snapshot().is_empty(), "epoch=0 must not broadcast");

        // Rejoin: epoch=1 → broadcast appended.
        let r2 = call(&h, Some("tx-1"), 60_000).await;
        assert_eq!(r2.producer_epoch, 1);
        let snap = log.snapshot();
        assert_eq!(snap.get(&r2.producer_id), Some(&1));

        // Second rejoin: epoch=2 → overwrites the prior entry.
        let r3 = call(&h, Some("tx-1"), 60_000).await;
        assert_eq!(r3.producer_epoch, 2);
        assert_eq!(log.snapshot().get(&r3.producer_id), Some(&2));
    }

    #[tokio::test]
    async fn empty_string_transactional_id_is_non_transactional() {
        // KIP-98 client convention: Some("") is equivalent to None.
        let h = InitProducerIdHandler::new(broker());
        let resp = call(&h, Some(""), 0).await;
        assert_eq!(resp.error_code, 0);
        assert!(resp.producer_id >= 1);
    }
}
