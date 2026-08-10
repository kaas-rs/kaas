//! DeleteTopics handler (key 20).
//!
//! Per topic, delete the `KafkaTopic` CR via
//! the installed [`TopicCRWriter`] (the operator's reconciler tears
//! down the partition dirs; the topic-watcher fires Deleted on every
//! broker so open handles close first), then drop the topic from the
//! in-memory registry. Without a CR writer (dev mode, unit tests)
//! only the registry removal runs.
//!
//! [`TopicCRWriter`]: crate::topic_cr_writer::TopicCRWriter

use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use kaas_auth::{Operation, Resource};
use kaas_codec::api::delete_topics;
use kaas_protocol::{ConnState, Handler, HandlerError};
use parking_lot::Mutex;

use super::principal_from;
use crate::broker::Broker;
use crate::topic_cr_writer::TopicWriteError;

const ERR_NONE: i16 = 0;
const ERR_UNKNOWN_TOPIC: i16 = 3;
const ERR_TOPIC_AUTHZ_FAILED: i16 = 29;
const ERR_INVALID_REQUEST: i16 = 42;

#[derive(Debug)]
pub struct DeleteTopicsHandler {
    broker: Arc<Broker>,
}

impl DeleteTopicsHandler {
    pub fn new(broker: Arc<Broker>) -> Self {
        Self { broker }
    }
}

#[async_trait]
impl Handler for DeleteTopicsHandler {
    async fn handle(
        &self,
        conn: &Mutex<ConnState>,
        version: i16,
        body: Bytes,
    ) -> Result<BytesMut, HandlerError> {
        let mut body = body;
        let req = delete_topics::decode_request(&mut body, version)?;
        let principal = principal_from(conn);

        let writer = self.broker.cr_writer();
        let mut responses = Vec::with_capacity(req.topic_names.len());
        for name in &req.topic_names {
            let mut result = delete_topics::DeletableTopicResult {
                name: name.clone(),
                error_code: ERR_NONE,
                error_message: None,
            };
            // gh #199: Apache requires `Delete` on the topic. Same
            // error-code convention as CreateTopics' gate (29).
            if !self.broker.authorizer.authorize(
                &principal,
                &Resource::topic(name),
                Operation::Delete,
            ) {
                result.error_code = ERR_TOPIC_AUTHZ_FAILED;
                responses.push(result);
                continue;
            }
            if let Some(w) = writer.as_ref() {
                match w.delete_topic(name).await {
                    Ok(()) => {}
                    Err(TopicWriteError::NotFound(_)) => {
                        result.error_code = ERR_UNKNOWN_TOPIC;
                    }
                    Err(e) => {
                        result.error_code = ERR_INVALID_REQUEST;
                        result.error_message = Some(e.to_string());
                    }
                }
                if result.error_code != ERR_NONE {
                    responses.push(result);
                    continue;
                }
            }
            self.broker.topics.remove(name);
            responses.push(result);
        }

        let resp = delete_topics::Response {
            throttle_time_ms: 0,
            responses,
        };
        let mut out = BytesMut::new();
        delete_topics::encode_response(&mut out, &resp, version)?;
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::handlers::test_authz::DenyAllAuthorizer;
    use crate::topic_registry::{TopicMeta, TopicRegistry};
    use kaas_storage::{MemoryStorage, StorageEngine};
    use std::net::SocketAddr;
    use std::str::FromStr;

    fn conn() -> Mutex<ConnState> {
        Mutex::new(ConnState::new(
            "internal",
            SocketAddr::from_str("127.0.0.1:9092").unwrap(),
        ))
    }

    /// gh #199: no `Delete` on the topic -> 29, and the topic
    /// survives in the registry.
    #[tokio::test]
    async fn denied_delete_returns_authz_failed_and_keeps_the_topic() {
        let engine: Arc<dyn StorageEngine> = Arc::new(MemoryStorage::new());
        let topics = Arc::new(TopicRegistry::new());
        topics.insert(TopicMeta {
            name: "t".into(),
            partition_count: 1,
            topic_id: [0; 16],
        });
        let b = Arc::new(Broker::with_auth(
            engine,
            topics.clone(),
            "test",
            0,
            Arc::new(DenyAllAuthorizer),
            Arc::new(kaas_auth::NoQuotaChecker),
        ));
        let h = DeleteTopicsHandler::new(b);

        let req = delete_topics::Request {
            topic_names: vec!["t".into()],
            timeout_ms: 1000,
        };
        let mut body = BytesMut::new();
        delete_topics::encode_request(&mut body, &req, 3).unwrap();
        let out = h.handle(&conn(), 3, body.freeze()).await.unwrap();
        let mut r = out.freeze();
        let resp = delete_topics::decode_response(&mut r, 3).unwrap();

        assert_eq!(resp.responses[0].error_code, ERR_TOPIC_AUTHZ_FAILED);
        assert!(
            topics.get("t").is_some(),
            "denied delete must not remove the topic from the registry"
        );
    }
}
