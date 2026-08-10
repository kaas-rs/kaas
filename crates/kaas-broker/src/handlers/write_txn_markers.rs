//! WriteTxnMarkers handler (key 27, v0–v1).
//!
//! Receiver-side marker writer. The txn coordinator sends one
//! [`Request`] per partition-leader broker after `EndTxn`; the
//! receiving broker validates leadership, builds a COMMIT / ABORT
//! control batch via [`build_control_batch`], and `engine.append`s
//! with `acks = -1` so the marker is durable before we ack.
//!
//! Cross-broker dispatch (the txn coord side) is gh #114. Phase 6
//! ships the receiver so an external coordinator could already
//! drive this; the same-broker fast path in [`EndTxnHandler`] writes
//! markers without round-tripping through this RPC.
//!
//! [`Request`]: kaas_codec::api::write_txn_markers::Request
//! [`build_control_batch`]: crate::control_batch::build_control_batch
//! [`EndTxnHandler`]: crate::handlers::EndTxnHandler

use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use kaas_auth::{Operation, Resource};
use kaas_codec::api::write_txn_markers;
use kaas_protocol::{ConnState, Handler, HandlerError};
use parking_lot::Mutex;

use super::principal_from;
use crate::broker::Broker;
use crate::control_batch::build_control_batch;

const ERR_NOT_LEADER_OR_FOLLOWER: i16 = 6;
const ERR_UNKNOWN_SERVER_ERROR: i16 = -1;
const ERR_CLUSTER_AUTHZ_FAILED: i16 = 31;
const ACKS_ALL: i16 = -1;

#[derive(Debug)]
pub struct WriteTxnMarkersHandler {
    broker: Arc<Broker>,
}

impl WriteTxnMarkersHandler {
    pub fn new(broker: Arc<Broker>) -> Self {
        Self { broker }
    }
}

#[async_trait]
impl Handler for WriteTxnMarkersHandler {
    async fn handle(
        &self,
        conn: &Mutex<ConnState>,
        version: i16,
        body: Bytes,
    ) -> Result<BytesMut, HandlerError> {
        let mut body = body;
        let req = write_txn_markers::decode_request(&mut body, version)?;

        // gh #199: Apache restricts this API to brokers via
        // `CLUSTER_ACTION` on the Cluster resource. Without the gate,
        // any authenticated client could append COMMIT/ABORT control
        // batches to partitions this broker leads — i.e. commit or
        // abort other producers' transactions. kaas coordinators
        // dispatch markers via the shared-PVC queue, never this RPC,
        // so with `simple` authorization nothing legitimate loses
        // access; grant `ClusterAction` to a principal explicitly to
        // drive this API from an external harness.
        let principal = principal_from(conn);
        let authorized = self.broker.authorizer.authorize(
            &principal,
            &Resource::cluster(),
            Operation::ClusterAction,
        );

        let mut markers_resp = Vec::with_capacity(req.markers.len());
        if !authorized {
            for marker in &req.markers {
                markers_resp.push(write_txn_markers::WritableTxnMarkerResult {
                    producer_id: marker.producer_id,
                    topics: marker
                        .topics
                        .iter()
                        .map(|topic| write_txn_markers::WritableTxnMarkerTopicResult {
                            name: topic.name.clone(),
                            partitions: topic
                                .partition_indexes
                                .iter()
                                .map(|&p| write_txn_markers::WritableTxnMarkerPartitionResult {
                                    partition_index: p,
                                    error_code: ERR_CLUSTER_AUTHZ_FAILED,
                                })
                                .collect(),
                        })
                        .collect(),
                });
            }
            let resp = write_txn_markers::Response {
                markers: markers_resp,
            };
            let mut out = BytesMut::new();
            write_txn_markers::encode_response(&mut out, &resp, version)?;
            return Ok(out);
        }
        for marker in &req.markers {
            let batch = Bytes::from(build_control_batch(
                marker.producer_id,
                marker.producer_epoch,
                marker.transaction_result,
                marker.coordinator_epoch,
            ));
            let mut topics_resp = Vec::with_capacity(marker.topics.len());
            for topic in &marker.topics {
                let mut partitions_resp = Vec::with_capacity(topic.partition_indexes.len());
                for &p in &topic.partition_indexes {
                    let code = self.write_one(&topic.name, p, &batch).await;
                    partitions_resp.push(write_txn_markers::WritableTxnMarkerPartitionResult {
                        partition_index: p,
                        error_code: code,
                    });
                }
                topics_resp.push(write_txn_markers::WritableTxnMarkerTopicResult {
                    name: topic.name.clone(),
                    partitions: partitions_resp,
                });
            }
            markers_resp.push(write_txn_markers::WritableTxnMarkerResult {
                producer_id: marker.producer_id,
                topics: topics_resp,
            });
        }

        let resp = write_txn_markers::Response {
            markers: markers_resp,
        };
        let mut out = BytesMut::new();
        write_txn_markers::encode_response(&mut out, &resp, version)?;
        Ok(out)
    }
}

impl WriteTxnMarkersHandler {
    async fn write_one(&self, topic: &str, partition: i32, batch: &Bytes) -> i16 {
        // Leadership gate. Dev mode (no Coordinator) treats every
        // broker as leader-of-all per the Phase-3/4 produce fallback.
        let owns = match self.broker.coordinator() {
            Some(c) => c.owns(topic, partition),
            None => true,
        };
        if !owns {
            return ERR_NOT_LEADER_OR_FOLLOWER;
        }
        // create_partition is idempotent; cheap safety net if a stale
        // coordinator dispatched to us before our partition was open.
        let _ = self.broker.engine.create_partition(topic, partition).await;
        let epoch = self
            .broker
            .coordinator()
            .and_then(|c| c.current_epoch(topic, partition))
            .unwrap_or_else(|| self.broker.local_lease.current_epoch());
        match self
            .broker
            .engine
            .append(topic, partition, epoch, ACKS_ALL, batch.clone())
            .await
        {
            Ok(_) => 0,
            Err(err) => {
                tracing::warn!(
                    topic,
                    partition,
                    %err,
                    "WriteTxnMarkers: control-batch append failed; \
                     consumers in read_committed mode will not see the txn as committed \
                     until the txn coordinator retries",
                );
                ERR_UNKNOWN_SERVER_ERROR
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::topic_registry::TopicRegistry;
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

    fn encode_request(req: &write_txn_markers::Request, version: i16) -> Bytes {
        use kaas_codec::api::common::{write_array_len, write_str};
        use kaas_codec::primitives::{write_i16, write_i32, write_i64, write_i8};
        use kaas_codec::tagged;
        let flexible = version >= write_txn_markers::MIN_FLEXIBLE;
        let mut w = BytesMut::new();
        write_array_len(&mut w, req.markers.len(), flexible).unwrap();
        for m in &req.markers {
            write_i64(&mut w, m.producer_id);
            write_i16(&mut w, m.producer_epoch);
            write_i8(&mut w, if m.transaction_result { 1 } else { 0 });
            write_array_len(&mut w, m.topics.len(), flexible).unwrap();
            for t in &m.topics {
                write_str(&mut w, &t.name, flexible).unwrap();
                write_array_len(&mut w, t.partition_indexes.len(), flexible).unwrap();
                for p in &t.partition_indexes {
                    write_i32(&mut w, *p);
                }
                if flexible {
                    tagged::write_empty(&mut w);
                }
            }
            write_i32(&mut w, m.coordinator_epoch);
            if flexible {
                tagged::write_empty(&mut w);
            }
        }
        if flexible {
            tagged::write_empty(&mut w);
        }
        w.freeze()
    }

    async fn call(
        h: &WriteTxnMarkersHandler,
        req: &write_txn_markers::Request,
    ) -> write_txn_markers::Response {
        let body = encode_request(req, 1);
        let out = h.handle(&conn(), 1, body).await.unwrap();
        let mut r = out.freeze();
        write_txn_markers::decode_response(&mut r, 1).unwrap()
    }

    /// gh #199: no `ClusterAction` on the Cluster resource -> 31 on
    /// every partition, and no control batch is appended.
    #[tokio::test]
    async fn denied_cluster_action_appends_nothing() {
        use crate::handlers::test_authz::DenyAllAuthorizer;
        let engine: Arc<dyn StorageEngine> = Arc::new(MemoryStorage::new());
        let b = Arc::new(Broker::with_auth(
            engine,
            Arc::new(TopicRegistry::new()),
            "test",
            0,
            Arc::new(DenyAllAuthorizer),
            Arc::new(kaas_auth::NoQuotaChecker),
        ));
        b.engine.create_partition("t", 0).await.unwrap();
        let hwm_before = b.engine.high_watermark("t", 0).unwrap();

        let h = WriteTxnMarkersHandler::new(b.clone());
        let req = write_txn_markers::Request {
            markers: vec![write_txn_markers::WritableTxnMarker {
                producer_id: 42,
                producer_epoch: 0,
                transaction_result: true,
                topics: vec![write_txn_markers::WritableTxnMarkerTopic {
                    name: "t".into(),
                    partition_indexes: vec![0],
                }],
                coordinator_epoch: 0,
            }],
        };
        let resp = call(&h, &req).await;
        assert_eq!(
            resp.markers[0].topics[0].partitions[0].error_code,
            ERR_CLUSTER_AUTHZ_FAILED
        );
        assert_eq!(
            b.engine.high_watermark("t", 0).unwrap(),
            hwm_before,
            "denied request must not append a control batch"
        );
    }

    #[tokio::test]
    async fn happy_path_appends_marker_per_partition() {
        let b = broker();
        b.engine.create_partition("t", 0).await.unwrap();
        b.engine.create_partition("t", 1).await.unwrap();
        let hwm_before_0 = b.engine.high_watermark("t", 0).unwrap();
        let hwm_before_1 = b.engine.high_watermark("t", 1).unwrap();

        let h = WriteTxnMarkersHandler::new(b.clone());
        let req = write_txn_markers::Request {
            markers: vec![write_txn_markers::WritableTxnMarker {
                producer_id: 42,
                producer_epoch: 3,
                transaction_result: true,
                topics: vec![write_txn_markers::WritableTxnMarkerTopic {
                    name: "t".into(),
                    partition_indexes: vec![0, 1],
                }],
                coordinator_epoch: 7,
            }],
        };
        let resp = call(&h, &req).await;
        for m in &resp.markers {
            for t in &m.topics {
                for p in &t.partitions {
                    assert_eq!(p.error_code, 0);
                }
            }
        }
        assert!(b.engine.high_watermark("t", 0).unwrap() > hwm_before_0);
        assert!(b.engine.high_watermark("t", 1).unwrap() > hwm_before_1);
    }

    #[tokio::test]
    async fn empty_marker_list_returns_empty_response() {
        let b = broker();
        let h = WriteTxnMarkersHandler::new(b);
        let req = write_txn_markers::Request { markers: vec![] };
        let resp = call(&h, &req).await;
        assert!(resp.markers.is_empty());
    }
}
