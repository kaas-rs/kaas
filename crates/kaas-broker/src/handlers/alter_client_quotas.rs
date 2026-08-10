//! AlterClientQuotas handler — API key 49 (KIP-546).
//!
//! Thin wrapper over [`QuotaEnforcer::set_user_quota`] /
//! [`QuotaEnforcer::describe_user_quota`]. One entry per `user`
//! entity; each entry carries a list of `(quota_key, value_or_remove)`
//! ops. The handler merges them into the user's current `Quotas`
//! struct, then installs the result as a runtime override.
//!
//! Per-entry errors:
//!
//! - non-user entity              → `INVALID_REQUEST` (42)
//! - unknown quota key            → `INVALID_CONFIG` (40)
//! - missing enforcer (no auth)   → `UNSUPPORTED_VERSION` (35)
//! - RBAC denial                  → `CLUSTER_AUTHORIZATION_FAILED` (31)
//!
//! `validate_only: true` skips the actual `set_user_quota` call.
//!
//! Authorization: `Operation::AlterConfigs` on the cluster
//! resource (Apache's mapping for quota-alter).

use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use kaas_auth::{Operation, QuotaEnforcer, Quotas, Resource};
use kaas_codec::api::alter_client_quotas::{
    self, EntityData, EntryData, EntryResponseData, OpData, Response,
};
use kaas_protocol::{ConnState, Handler, HandlerError};
use parking_lot::Mutex;

use super::principal_from;
use crate::broker::Broker;

const ERR_NONE: i16 = 0;
const ERR_CLUSTER_AUTHZ_FAILED: i16 = 31;
const ERR_INVALID_CONFIG: i16 = 40;
const ERR_INVALID_REQUEST: i16 = 42;
const ERR_UNSUPPORTED_VERSION: i16 = 35;

#[derive(Debug)]
pub struct AlterClientQuotasHandler {
    broker: Arc<Broker>,
}

impl AlterClientQuotasHandler {
    pub fn new(broker: Arc<Broker>) -> Self {
        Self { broker }
    }
}

#[async_trait]
impl Handler for AlterClientQuotasHandler {
    async fn handle(
        &self,
        conn: &Mutex<ConnState>,
        version: i16,
        body: Bytes,
    ) -> Result<BytesMut, HandlerError> {
        let mut body = body;
        let req = alter_client_quotas::decode_request(&mut body, version)?;

        let principal = principal_from(conn);
        let mut entries = Vec::with_capacity(req.entries.len());

        // Cluster-level authz: one decision covers all entries.
        let cluster_authzed = self.broker.authorizer.authorize(
            &principal,
            &Resource::cluster(),
            Operation::AlterConfigs,
        );
        let enforcer = self.broker.quota_enforcer();

        for entry in req.entries {
            if !cluster_authzed {
                entries.push(error_entry(&entry, ERR_CLUSTER_AUTHZ_FAILED, None));
                continue;
            }
            let Some(enf) = enforcer.as_ref() else {
                entries.push(error_entry(
                    &entry,
                    ERR_UNSUPPORTED_VERSION,
                    Some("broker has no quota enforcer"),
                ));
                continue;
            };
            // kaas only supports a single `user` axis. Reject anything else.
            let Some(username) = exact_user(&entry.entity) else {
                entries.push(error_entry(
                    &entry,
                    ERR_INVALID_REQUEST,
                    Some("only 'user' entity_type with an explicit name is supported"),
                ));
                continue;
            };

            // Merge ops onto the existing quotas (Apache semantics:
            // a Set replaces just the named key; a Remove drops just
            // that key; unspecified keys are preserved).
            let mut current = enf.describe_user_quota(&username).unwrap_or_default();
            let mut bad_key: Option<String> = None;
            for op in &entry.ops {
                if !apply_op(&mut current, op) {
                    bad_key = Some(op.key.clone());
                    break;
                }
            }
            if let Some(key) = bad_key {
                entries.push(error_entry(
                    &entry,
                    ERR_INVALID_CONFIG,
                    Some(&format!("unknown quota key: {key}")),
                ));
                continue;
            }

            if req.validate_only {
                entries.push(ok_entry(&entry));
                continue;
            }

            // If every field is None after the merge, clear the
            // override entirely; otherwise install the merged Quotas.
            let next = if quotas_is_empty(&current) {
                None
            } else {
                Some(current)
            };
            enf.set_user_quota(&username, next);
            entries.push(ok_entry(&entry));
        }

        encode(
            version,
            Response {
                throttle_time_ms: 0,
                entries,
            },
        )
    }
}

fn apply_op(q: &mut Quotas, op: &OpData) -> bool {
    match op.key.as_str() {
        "producer_byte_rate" => {
            q.producer_max_byte_rate_per_broker = op.value.map(f64_to_i64);
            true
        }
        "consumer_byte_rate" => {
            q.consumer_max_byte_rate_per_broker = op.value.map(f64_to_i64);
            true
        }
        "request_percentage" => {
            q.request_percentage = op.value.map(f64_to_i32);
            true
        }
        _ => false,
    }
}

fn exact_user(axes: &[EntityData]) -> Option<String> {
    let mut user_axes = axes.iter().filter(|a| a.entity_type == "user");
    let first = user_axes.next()?;
    // Reject if there are multiple user axes or extra non-user axes.
    if user_axes.next().is_some() {
        return None;
    }
    if axes.iter().any(|a| a.entity_type != "user") {
        return None;
    }
    first.entity_name.clone()
}

fn quotas_is_empty(q: &Quotas) -> bool {
    q.producer_max_byte_rate_per_broker.is_none()
        && q.consumer_max_byte_rate_per_broker.is_none()
        && q.request_percentage.is_none()
}

fn ok_entry(entry: &EntryData) -> EntryResponseData {
    EntryResponseData {
        error_code: ERR_NONE,
        error_message: None,
        entity: entry.entity.clone(),
    }
}

fn error_entry(entry: &EntryData, code: i16, msg: Option<&str>) -> EntryResponseData {
    EntryResponseData {
        error_code: code,
        error_message: msg.map(str::to_owned),
        entity: entry.entity.clone(),
    }
}

fn encode(version: i16, resp: Response) -> Result<BytesMut, HandlerError> {
    let mut out = BytesMut::new();
    alter_client_quotas::encode_response(&mut out, &resp, version)?;
    Ok(out)
}

fn f64_to_i64(v: f64) -> i64 {
    // Saturate at the i64 range. f64 → i64 via min/max + parse-round.
    if v.is_nan() {
        return 0;
    }
    if v >= i64_max_f64() {
        return i64::MAX;
    }
    if v <= i64_min_f64() {
        return i64::MIN;
    }
    // Rounded toward zero, well-defined in the [i64::MIN, i64::MAX] range.
    f64_trunc_to_i64(v)
}

fn f64_to_i32(v: f64) -> i32 {
    if v.is_nan() {
        return 0;
    }
    if v >= f64::from(i32::MAX) {
        return i32::MAX;
    }
    if v <= f64::from(i32::MIN) {
        return i32::MIN;
    }
    f64_trunc_to_i32(v)
}

// Float-to-int helpers wrapped to keep the `as` cast localised to a
// single function each, with explicit bounds checks above.
#[allow(clippy::cast_possible_truncation, clippy::as_conversions)]
fn f64_trunc_to_i64(v: f64) -> i64 {
    v.trunc() as i64
}

#[allow(clippy::cast_possible_truncation, clippy::as_conversions)]
fn f64_trunc_to_i32(v: f64) -> i32 {
    v.trunc() as i32
}

// `f64::from(i64::MAX)` does not exist (lossy conversion). Express
// the bound directly via `2^63`.
fn i64_max_f64() -> f64 {
    9_223_372_036_854_775_807.0
}
fn i64_min_f64() -> f64 {
    -9_223_372_036_854_775_808.0
}

/// `QuotaEnforcer` re-export for handler-internal use; lets the
/// rustdoc link resolve without re-exporting from `Broker`.
#[allow(dead_code)]
type _Enforcer = QuotaEnforcer;
