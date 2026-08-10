//! DeleteRecords handler (key 21).
//!
//! The broker side of `kafka-delete-records.sh` / Kafbat's "Purge
//! messages": per partition, advance the log start offset to the
//! caller's target (KIP-107; `-1` = purge to HWM) so earlier records
//! become invisible to Fetch and eligible for retention cleanup.
//!
//! Ownership gate mirrors Produce's: with a cluster [`Coordinator`]
//! wired, requests for partitions this broker doesn't lead answer
//! `NOT_LEADER_OR_FOLLOWER` (6); without one (dev mode) the storage
//! engine's own unknown-partition error covers the miss.
//!
//! [`Coordinator`]: crate::coordinator::Coordinator

use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use kaas_auth::{Operation, Resource};
use kaas_codec::api::delete_records;
use kaas_protocol::{ConnState, Handler, HandlerError};
use kaas_storage::StorageError;
use parking_lot::Mutex;

use super::principal_from;
use crate::broker::Broker;

const ERR_NONE: i16 = 0;
const ERR_OFFSET_OUT_OF_RANGE: i16 = 1;
const ERR_UNKNOWN_TOPIC: i16 = 3;
const ERR_NOT_LEADER: i16 = 6;
const ERR_TOPIC_AUTHZ_FAILED: i16 = 29;

#[derive(Debug)]
pub struct DeleteRecordsHandler {
    broker: Arc<Broker>,
}

impl DeleteRecordsHandler {
    pub fn new(broker: Arc<Broker>) -> Self {
        Self { broker }
    }
}

#[async_trait]
impl Handler for DeleteRecordsHandler {
    async fn handle(
        &self,
        conn: &Mutex<ConnState>,
        version: i16,
        body: Bytes,
    ) -> Result<BytesMut, HandlerError> {
        let mut body = body;
        let req = delete_records::decode_request(&mut body, version)?;
        let principal = principal_from(conn);

        let coordinator = self.broker.coordinator();
        let mut topics = Vec::with_capacity(req.topics.len());
        for topic in &req.topics {
            // gh #199: Apache requires `Delete` on the topic — this
            // API destroys data (advances logStart past records).
            let authorized = self.broker.authorizer.authorize(
                &principal,
                &Resource::topic(&topic.name),
                Operation::Delete,
            );
            let mut partitions = Vec::with_capacity(topic.partitions.len());
            for p in &topic.partitions {
                let mut pr = delete_records::DeleteRecordsPartitionResult {
                    partition_index: p.partition_index,
                    low_watermark: -1,
                    error_code: ERR_NONE,
                };
                if !authorized {
                    pr.error_code = ERR_TOPIC_AUTHZ_FAILED;
                    partitions.push(pr);
                    continue;
                }
                if let Some(c) = coordinator.as_ref() {
                    if !c.owns(&topic.name, p.partition_index) {
                        pr.error_code = ERR_NOT_LEADER;
                        partitions.push(pr);
                        continue;
                    }
                }
                match self
                    .broker
                    .engine
                    .delete_records(&topic.name, p.partition_index, p.offset)
                    .await
                {
                    Ok(low_watermark) => pr.low_watermark = low_watermark,
                    Err(StorageError::OffsetOutOfRange) => {
                        pr.error_code = ERR_OFFSET_OUT_OF_RANGE;
                    }
                    Err(_) => pr.error_code = ERR_UNKNOWN_TOPIC,
                }
                partitions.push(pr);
            }
            topics.push(delete_records::DeleteRecordsTopicResult {
                name: topic.name.clone(),
                partitions,
            });
        }

        let resp = delete_records::Response {
            throttle_time_ms: 0,
            topics,
        };
        let mut out = BytesMut::new();
        delete_records::encode_response(&mut out, &resp, version)?;
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::handlers::test_authz::DenyAllAuthorizer;
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

    /// gh #199: no `Delete` on the topic -> 29 per partition, and the
    /// log-start offset stays where it was.
    #[tokio::test]
    async fn denied_delete_returns_authz_failed_and_purges_nothing() {
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
        let h = DeleteRecordsHandler::new(b.clone());

        let req = delete_records::Request {
            topics: vec![delete_records::DeleteRecordsTopic {
                name: "t".into(),
                partitions: vec![delete_records::DeleteRecordsPartition {
                    partition_index: 0,
                    offset: -1,
                }],
            }],
            timeout_ms: 1000,
        };
        let mut body = BytesMut::new();
        delete_records::encode_request(&mut body, &req, 2).unwrap();
        let out = h.handle(&conn(), 2, body.freeze()).await.unwrap();
        let mut r = out.freeze();
        let resp = delete_records::decode_response(&mut r, 2).unwrap();

        let pr = &resp.topics[0].partitions[0];
        assert_eq!(pr.error_code, ERR_TOPIC_AUTHZ_FAILED);
        assert_eq!(pr.low_watermark, -1, "no purge may have happened");
    }
}
