//! `KafkaUser` — Strimzi-shape user CR.
//!
//! Carries
//! per-user authentication (`scram-sha-512` / `tls` /
//! `kubernetes-serviceaccount`), inline `spec.authorization.acls`
//! (post-gh #135 — `KafkaACL` and `KafkaUserGroup` are gone), and
//! per-broker quotas (gh #126 — named honestly to advertise the
//! per-broker semantics rather than Strimzi's `producerByteRate`
//! cluster-wide-looking name).
//!
//! Per the gh #137 closure, ACL-shape fields (`type`, `patternType`)
//! are **not** apiserver-defaulted via `#[schemars(default = ...)]`.
//! Defaults live in the operator-side `acl_to_entry` translation
//! step (`kaas-operator-controllers::acls`).

use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::condition::Condition;

#[derive(CustomResource, Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[kube(
    group = "kaas.rs",
    version = "v1alpha1",
    kind = "KafkaUser",
    plural = "kafkausers",
    singular = "kafkauser",
    namespaced,
    status = "KafkaUserStatus",
    printcolumn = r#"{"name":"Auth type","type":"string","jsonPath":".spec.authentication.type"}"#,
    printcolumn = r#"{"name":"Ready","type":"string","jsonPath":".status.conditions[?(@.type=='Ready')].status"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct KafkaUserSpec {
    /// Optional (gh #42): an **authorization-only** user omits it. A
    /// principal that authenticates out-of-band — an OAUTHBEARER
    /// token validated against the issuer's JWKS — has no credential
    /// for the operator to materialize, so its `KafkaUser` carries
    /// only `authorization`/`quotas` and names the principal via
    /// `metadata.name` (e.g. the token's `sub`). This mirrors
    /// Strimzi, whose oauth users are authorization-only too. When
    /// present, the credential is written to `credentials.json` as
    /// before; when absent, only ACLs/quotas are reconciled.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authentication: Option<KafkaUserAuthentication>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authorization: Option<KafkaUserAuthorization>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quotas: Option<KafkaUserQuotas>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct KafkaUserAuthentication {
    /// `scram-sha-512`, `tls`, or `kubernetes-serviceaccount`.
    #[serde(rename = "type")]
    #[schemars(regex(pattern = r"^(scram-sha-512|tls|kubernetes-serviceaccount)$"))]
    pub kind: String,

    /// Input Secret carrying the SCRAM password (gh #136 — optional;
    /// when unset the operator auto-generates a stable 32-char
    /// password into the output Secret `<user>-kafka-credentials`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub password: Option<SecretKeyRef>,

    /// Pre-derived SCRAM credential (gh #104, KIP-554). When set,
    /// the operator passes the salt/storedKey/serverKey/iterations
    /// through to `credentials.json` verbatim and skips PBKDF2 —
    /// the wire-level admin path uses this to rotate runtime
    /// credentials without an intermediate Secret.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scram: Option<KafkaUserScramCredential>,

    /// Used when `type = tls`. The operator stamps the CN into
    /// `credentials.json` as the mTLS principal.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub certificate_ref: Option<LocalObjectRef>,

    /// Used when `type = kubernetes-serviceaccount`. Wired in
    /// Phase 7 alongside the JWT validation path.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service_account_ref: Option<ServiceAccountRef>,
}

/// Pre-derived SCRAM credential. All fields base64; opaque to the
/// operator (passed through to `credentials.json`).
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct KafkaUserScramCredential {
    pub salt: String,
    pub stored_key: String,
    pub server_key: String,
    pub iterations: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct SecretKeyRef {
    pub name: String,
    pub key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct LocalObjectRef {
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct ServiceAccountRef {
    pub name: String,
    pub namespace: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct KafkaUserAuthorization {
    /// `simple` today. Field is reserved for forward compat (OPA / OIDC).
    /// **Not** apiserver-defaulted — gh #137. The operator treats
    /// empty as "simple" at reconcile time.
    #[serde(default, rename = "type", skip_serializing_if = "String::is_empty")]
    #[schemars(regex(pattern = r"^(simple)?$"))]
    pub kind: String,

    pub acls: Vec<KafkaUserAcl>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct KafkaUserAcl {
    pub resource: KafkaUserAclResource,

    #[schemars(length(min = 1))]
    pub operations: Vec<String>,

    /// `allow` or `deny`. Empty → operator-side default `allow`
    /// (gh #137 — no apiserver defaulting).
    #[serde(default, rename = "type", skip_serializing_if = "String::is_empty")]
    #[schemars(regex(pattern = r"^(allow|deny)?$"))]
    pub kind: String,

    /// Source-IP filter. Empty = any host (Apache `*` wildcard).
    /// Stored for round-trip but ignored at the broker (only "any"
    /// is enforced today).
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub host: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct KafkaUserAclResource {
    /// `topic`, `group`, `cluster`, or `transactionalId`.
    #[serde(rename = "type")]
    #[schemars(regex(pattern = r"^(topic|group|cluster|transactionalId)$"))]
    pub kind: String,

    pub name: String,

    /// `literal` or `prefix`. Empty → operator-side default `literal`
    /// (gh #137).
    #[serde(default, skip_serializing_if = "String::is_empty")]
    #[schemars(regex(pattern = r"^(literal|prefix)?$"))]
    pub pattern_type: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct KafkaUserQuotas {
    /// Producer cap at THIS broker only (Apache Kafka 3.7 / KIP-13
    /// per-broker semantics). With N brokers the effective
    /// cluster-wide ceiling is N × this. Gh #126 — the named-
    /// honestly divergence from Strimzi's `producerByteRate`.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 0))]
    pub producer_max_byte_rate_per_broker: Option<i64>,

    /// Consumer cap at THIS broker only.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 0))]
    pub consumer_max_byte_rate_per_broker: Option<i64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 0, max = 100))]
    pub request_percentage: Option<i32>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct KafkaUserStatus {
    /// Output Secret name (gh #136 — populated for SCRAM users whose
    /// password the operator auto-generated).
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub secret: String,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<Condition>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn authorization_only_user_parses_without_authentication() {
        // gh #42: an OAUTHBEARER principal has no credential for the
        // operator to materialize, so its KafkaUser omits
        // `authentication` and carries only `authorization`.
        let spec: KafkaUserSpec = serde_json::from_str(
            r#"{
                "authorization": {
                    "type": "simple",
                    "acls": [
                        {"resource": {"type": "topic", "name": "kaas-canary-v1"},
                         "operations": ["Read", "Write", "Describe"]}
                    ]
                }
            }"#,
        )
        .unwrap();
        assert!(spec.authentication.is_none());
        assert!(spec.authorization.is_some());
    }

    #[test]
    fn authenticated_user_still_parses() {
        let spec: KafkaUserSpec =
            serde_json::from_str(r#"{"authentication": {"type": "scram-sha-512"}}"#).unwrap();
        assert_eq!(spec.authentication.unwrap().kind, "scram-sha-512");
    }
}
