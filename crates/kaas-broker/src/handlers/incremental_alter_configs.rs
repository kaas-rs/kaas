//! IncrementalAlterConfigs handler — API key 44 (gh #9).
//!
//! Topic-only surface (BROKER + BROKER_LOGGER return
//! `UNSUPPORTED_VERSION` (35)). Per-resource error codes ride in
//! the response; the top-level `error_code` stays 0.
//!
//! Each op gets translated by the installed [`TopicCRWriter`] into
//! a JSON-merge PATCH on `KafkaTopic.spec.config`. `Set` writes
//! the parsed value, `Delete` writes null, `Append` / `Subtract`
//! surface as `UNSUPPORTED_VERSION` (kaas's topic configs are
//! scalar — list-valued ops don't apply).
//!
//! Authorization: `Operation::AlterConfigs` on the topic resource.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use kaas_auth::{Operation, Resource};
use kaas_codec::api::incremental_alter_configs;
use kaas_protocol::{ConnState, Handler, HandlerError};
use parking_lot::Mutex;

use super::principal_from;
use crate::broker::Broker;
use crate::topic_cr_writer::{ConfigOpKind, ConfigOpWithValue, TopicWriteError};

const ERR_NONE: i16 = 0;
const ERR_UNKNOWN_TOPIC: i16 = 3;
const ERR_CLUSTER_AUTHZ_FAILED: i16 = 31;
const ERR_TOPIC_AUTHZ_FAILED: i16 = 29;
const ERR_UNSUPPORTED_VERSION: i16 = 35;
const ERR_UNKNOWN_SERVER: i16 = -1;

#[derive(Debug)]
pub struct IncrementalAlterConfigsHandler {
    broker: Arc<Broker>,
}

impl IncrementalAlterConfigsHandler {
    pub fn new(broker: Arc<Broker>) -> Self {
        Self { broker }
    }
}

#[async_trait]
impl Handler for IncrementalAlterConfigsHandler {
    async fn handle(
        &self,
        conn: &Mutex<ConnState>,
        version: i16,
        body: Bytes,
    ) -> Result<BytesMut, HandlerError> {
        let mut body = body;
        let req = incremental_alter_configs::decode_request(&mut body, version)?;

        let principal = principal_from(conn);
        let writer = self.broker.cr_writer();
        let mut responses = Vec::with_capacity(req.resources.len());

        for resource in req.resources {
            if resource.resource_type != incremental_alter_configs::resource_type::TOPIC {
                // BROKER + BROKER_LOGGER not implemented — the
                // CLAUDE.md note pins this as a non-goal until the
                // broker grows a dynamic-config surface.
                responses.push(response_for(
                    &resource,
                    ERR_UNSUPPORTED_VERSION,
                    Some("only Topic resource type is supported"),
                ));
                continue;
            }

            let res = Resource::topic(&resource.resource_name);
            if !self
                .broker
                .authorizer
                .authorize(&principal, &res, Operation::AlterConfigs)
            {
                responses.push(response_for(&resource, ERR_TOPIC_AUTHZ_FAILED, None));
                continue;
            }

            let Some(w) = writer.as_ref() else {
                responses.push(response_for(
                    &resource,
                    ERR_CLUSTER_AUTHZ_FAILED,
                    Some("broker is not running in cluster mode"),
                ));
                continue;
            };

            let ops: Vec<ConfigOpWithValue> = resource
                .configs
                .iter()
                .map(|c| ConfigOpWithValue {
                    key: c.name.clone(),
                    kind: wire_op_to_kind(c.op),
                    value: c.value.clone(),
                })
                .collect();

            if req.validate_only {
                responses.push(response_for(&resource, ERR_NONE, None));
                continue;
            }

            match w.update_topic_config(&resource.resource_name, &ops).await {
                Ok(()) => responses.push(response_for(&resource, ERR_NONE, None)),
                Err(TopicWriteError::NotFound(_)) => {
                    responses.push(response_for(&resource, ERR_UNKNOWN_TOPIC, None))
                }
                Err(TopicWriteError::UnsupportedOp(kind)) => responses.push(response_for(
                    &resource,
                    ERR_UNSUPPORTED_VERSION,
                    Some(&format!("unsupported op: {kind:?}")),
                )),
                Err(TopicWriteError::Forbidden(msg)) => responses.push(response_for(
                    &resource,
                    ERR_CLUSTER_AUTHZ_FAILED,
                    Some(&msg),
                )),
                Err(other) => {
                    tracing::warn!(error = %other, topic = %resource.resource_name, "IncrementalAlterConfigs failed");
                    responses.push(response_for(&resource, ERR_UNKNOWN_SERVER, None));
                }
            }
        }

        let resp = incremental_alter_configs::Response {
            throttle_time_ms: 0,
            responses,
        };
        let mut out = BytesMut::new();
        incremental_alter_configs::encode_response(&mut out, &resp, version)?;
        Ok(out)
    }
}

fn wire_op_to_kind(op: i8) -> ConfigOpKind {
    match op {
        incremental_alter_configs::op::SET => ConfigOpKind::Set,
        incremental_alter_configs::op::DELETE => ConfigOpKind::Delete,
        incremental_alter_configs::op::APPEND => ConfigOpKind::Append,
        incremental_alter_configs::op::SUBTRACT => ConfigOpKind::Subtract,
        // Unknown op discriminant — bias toward Set so the writer
        // can decide; in practice this never happens because the
        // codec validates the i8 against the schema.
        _ => ConfigOpKind::Set,
    }
}

fn response_for(
    resource: &incremental_alter_configs::AlterConfigsResource,
    code: i16,
    message: Option<&str>,
) -> incremental_alter_configs::AlterConfigsResourceResponse {
    incremental_alter_configs::AlterConfigsResourceResponse {
        error_code: code,
        error_message: message.map(str::to_owned),
        resource_type: resource.resource_type,
        resource_name: resource.resource_name.clone(),
    }
}
