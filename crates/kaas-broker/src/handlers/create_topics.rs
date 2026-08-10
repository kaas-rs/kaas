//! CreateTopics handler — API key 19 (gh #51).
//!
//! Translates `AdminClient.createTopics()` / `kafka-topics.sh
//! --create` into a POST that mints a fresh `KafkaTopic` CR via the
//! installed [`TopicCRWriter`]. The operator then reconciles the
//! CR, creating the topic's on-disk partition directories on the
//! shared PVC; the broker observes the new KafkaTopic via its
//! existing `TopicWatcher` and serves Produce/Fetch against it on
//! subsequent requests.
//!
//! Authorization: `Operation::Create` on the topic resource.
//!
//! Wire error mapping (see also [`TopicWriteError`]):
//!
//! - authorization denied → `TOPIC_AUTHORIZATION_FAILED` (29)
//! - missing CR writer    → `CLUSTER_AUTHORIZATION_FAILED` (31)
//! - CR already exists    → `TOPIC_ALREADY_EXISTS` (36)
//! - RBAC denial          → `CLUSTER_AUTHORIZATION_FAILED` (31)
//! - other kube error     → `UNKNOWN_SERVER_ERROR` (-1)
//!
//! `validate_only: true` (v1+) short-circuits BEFORE minting the CR —
//! the authorization + writer checks still run and the would-be
//! response is returned without mutating state.
//!
//! Config overrides on the wire request (retention.ms etc.) are
//! parsed for protocol fidelity but currently ignored. A follow-up
//! patch will thread them through as a second `spec.config` patch
//! after the initial POST — the operator materialises the same
//! knobs on reconcile.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use kaas_auth::{Operation, Resource};
use kaas_codec::api::create_topics;
use kaas_protocol::{ConnState, Handler, HandlerError};
use parking_lot::Mutex;

use super::principal_from;
use crate::broker::Broker;
use crate::topic_cr_writer::TopicWriteError;

const ERR_NONE: i16 = 0;
const ERR_TOPIC_ALREADY_EXISTS: i16 = 36;
const ERR_CLUSTER_AUTHZ_FAILED: i16 = 31;
const ERR_TOPIC_AUTHZ_FAILED: i16 = 29;
const ERR_UNKNOWN_SERVER: i16 = -1;

#[derive(Debug)]
pub struct CreateTopicsHandler {
    broker: Arc<Broker>,
}

impl CreateTopicsHandler {
    pub fn new(broker: Arc<Broker>) -> Self {
        Self { broker }
    }
}

#[async_trait]
impl Handler for CreateTopicsHandler {
    async fn handle(
        &self,
        conn: &Mutex<ConnState>,
        version: i16,
        body: Bytes,
    ) -> Result<BytesMut, HandlerError> {
        let mut body = body;
        let req = create_topics::decode_request(&mut body, version)?;

        let principal = principal_from(conn);
        let writer = self.broker.cr_writer();
        let mut topics = Vec::with_capacity(req.topics.len());

        for topic in req.topics {
            // Determine effective partition count: negative (typical
            // AdminClient shape when caller wants "server default")
            // maps to 1 partition — mirrors Apache's
            // `num.partitions=1` default.
            let num_partitions = if topic.num_partitions <= 0 {
                1
            } else {
                topic.num_partitions
            };
            let replication_factor = if topic.replication_factor <= 0 {
                1
            } else {
                topic.replication_factor
            };

            let resource = Resource::topic(&topic.name);
            if !self
                .broker
                .authorizer
                .authorize(&principal, &resource, Operation::Create)
            {
                topics.push(create_topics::CreatableTopicResult::new(
                    &topic.name,
                    ERR_TOPIC_AUTHZ_FAILED,
                ));
                continue;
            }

            let Some(w) = writer.as_ref() else {
                topics.push(
                    create_topics::CreatableTopicResult::new(&topic.name, ERR_CLUSTER_AUTHZ_FAILED)
                        .with_error_message("broker is not running in cluster mode"),
                );
                continue;
            };

            if req.validate_only {
                topics.push(
                    create_topics::CreatableTopicResult::new(&topic.name, ERR_NONE)
                        .with_created(num_partitions, replication_factor),
                );
                continue;
            }

            match w.create_topic(&topic.name, num_partitions).await {
                Ok(()) => topics.push(
                    create_topics::CreatableTopicResult::new(&topic.name, ERR_NONE)
                        .with_created(num_partitions, replication_factor),
                ),
                Err(TopicWriteError::AlreadyExists(_)) => topics.push(
                    create_topics::CreatableTopicResult::new(&topic.name, ERR_TOPIC_ALREADY_EXISTS)
                        .with_error_message("topic already exists"),
                ),
                Err(TopicWriteError::Forbidden(msg)) => topics.push(
                    create_topics::CreatableTopicResult::new(&topic.name, ERR_CLUSTER_AUTHZ_FAILED)
                        .with_error_message(msg),
                ),
                Err(other) => {
                    tracing::warn!(error = %other, topic = %topic.name, "CreateTopics failed");
                    topics.push(create_topics::CreatableTopicResult::new(
                        &topic.name,
                        ERR_UNKNOWN_SERVER,
                    ));
                }
            }
        }

        let resp = create_topics::Response {
            throttle_time_ms: 0,
            topics,
        };
        let mut out = BytesMut::new();
        create_topics::encode_response(&mut out, &resp, version)?;
        Ok(out)
    }
}
