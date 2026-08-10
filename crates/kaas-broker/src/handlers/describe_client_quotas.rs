//! DescribeClientQuotas handler — API key 48 (KIP-546).
//!
//! Thin wrapper over [`QuotaEnforcer::describe_user_quota`] /
//! [`QuotaEnforcer::list_user_quotas`]. kaas only supports the
//! `user` entity axis — `client-id` / `ip` filters return an empty
//! response (the wire schema doesn't carry a per-axis error code).
//!
//! Filter semantics:
//!
//! - One component `(entity_type=user, match_type=EXACT, match=<name>)`
//!   → describe that user only.
//! - `match_type=ANY` (no `match`) → list every user with a quota.
//! - `match_type=DEFAULT` → not supported; returns empty.
//!
//! Authorization: `Operation::DescribeConfigs` on the cluster
//! resource (Apache's mapping for quota-describe).

use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use kaas_auth::{Operation, Quotas, Resource};
use kaas_codec::api::describe_client_quotas::{
    self, entity_type, match_type, ComponentData, EntityData, EntryData, Response, ValueData,
};
use kaas_protocol::{ConnState, Handler, HandlerError};
use parking_lot::Mutex;

use super::principal_from;
use crate::broker::Broker;

const ERR_NONE: i16 = 0;
const ERR_CLUSTER_AUTHZ_FAILED: i16 = 31;

#[derive(Debug)]
pub struct DescribeClientQuotasHandler {
    broker: Arc<Broker>,
}

impl DescribeClientQuotasHandler {
    pub fn new(broker: Arc<Broker>) -> Self {
        Self { broker }
    }
}

#[async_trait]
impl Handler for DescribeClientQuotasHandler {
    async fn handle(
        &self,
        conn: &Mutex<ConnState>,
        version: i16,
        body: Bytes,
    ) -> Result<BytesMut, HandlerError> {
        let mut body = body;
        let req = describe_client_quotas::decode_request(&mut body, version)?;

        let principal = principal_from(conn);
        // Apache maps quota describe → cluster:DescribeConfigs.
        if !self.broker.authorizer.authorize(
            &principal,
            &Resource::cluster(),
            Operation::DescribeConfigs,
        ) {
            return encode(version, error_response(ERR_CLUSTER_AUTHZ_FAILED, "denied"));
        }

        let Some(enforcer) = self.broker.quota_enforcer() else {
            // No real enforcer wired (auth disabled or
            // NoQuotaChecker). The schema doesn't carry a way to
            // distinguish "no quotas" from "no enforcer"; mirror
            // Apache and return an empty success.
            return encode(version, empty_response());
        };

        let entries = match resolve_user_filter(&req.components) {
            UserFilter::Exact(username) => match enforcer.describe_user_quota(&username) {
                Some(q) => vec![user_entry(&username, &q)],
                None => vec![],
            },
            UserFilter::Any => enforcer
                .list_user_quotas()
                .into_iter()
                .map(|(u, q)| user_entry(&u, &q))
                .collect(),
            UserFilter::Unsupported => vec![],
        };

        encode(
            version,
            Response {
                throttle_time_ms: 0,
                error_code: ERR_NONE,
                error_message: None,
                entries,
            },
        )
    }
}

#[derive(Debug)]
enum UserFilter {
    Exact(String),
    Any,
    Unsupported,
}

fn resolve_user_filter(components: &[ComponentData]) -> UserFilter {
    // kaas supports a single `user` axis. Anything else → Unsupported.
    if components.is_empty() {
        return UserFilter::Any;
    }
    let user_components: Vec<&ComponentData> = components
        .iter()
        .filter(|c| c.entity_type == entity_type::USER)
        .collect();
    if user_components.is_empty() {
        return UserFilter::Unsupported;
    }
    if user_components.len() > 1 {
        // Multiple `user` components is malformed; mirror Apache —
        // empty match set.
        return UserFilter::Unsupported;
    }
    let c = user_components[0];
    match c.match_type {
        match_type::EXACT => match c.match_.as_deref() {
            Some(n) => UserFilter::Exact(n.to_string()),
            None => UserFilter::Unsupported,
        },
        match_type::ANY => UserFilter::Any,
        // DEFAULT (1) means "the <default> user entity"; kaas has
        // no such notion (users are CR-instantiated). Return empty.
        _ => UserFilter::Unsupported,
    }
}

fn user_entry(username: &str, q: &Quotas) -> EntryData {
    let mut values = Vec::new();
    if let Some(p) = q.producer_max_byte_rate_per_broker {
        values.push(ValueData {
            key: "producer_byte_rate".into(),
            value: f64_from_i64(p),
        });
    }
    if let Some(c) = q.consumer_max_byte_rate_per_broker {
        values.push(ValueData {
            key: "consumer_byte_rate".into(),
            value: f64_from_i64(c),
        });
    }
    if let Some(rp) = q.request_percentage {
        values.push(ValueData {
            key: "request_percentage".into(),
            value: f64::from(rp),
        });
    }
    EntryData {
        entity: vec![EntityData {
            entity_type: entity_type::USER.into(),
            entity_name: Some(username.into()),
        }],
        values,
    }
}

fn empty_response() -> Response {
    Response {
        throttle_time_ms: 0,
        error_code: ERR_NONE,
        error_message: None,
        entries: vec![],
    }
}

fn error_response(code: i16, msg: &str) -> Response {
    Response {
        throttle_time_ms: 0,
        error_code: code,
        error_message: Some(msg.into()),
        entries: vec![],
    }
}

fn encode(version: i16, resp: Response) -> Result<BytesMut, HandlerError> {
    let mut out = BytesMut::new();
    describe_client_quotas::encode_response(&mut out, &resp, version)?;
    Ok(out)
}

#[allow(clippy::cast_precision_loss, clippy::as_conversions)]
fn f64_from_i64(v: i64) -> f64 {
    // Quotas always fit in 53-bit f64 mantissa range in practice
    // (max byte rate on a 100 GbE NIC is ~1e10 bytes/sec). Precision
    // loss only kicks in above 2^53 ≈ 9 PiB/s. Localised `as` cast.
    v as f64
}
