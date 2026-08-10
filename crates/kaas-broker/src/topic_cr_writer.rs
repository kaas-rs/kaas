//! `TopicCRWriter` — broker → `KafkaTopic` CR patch path.
//!
//! The admin handlers `CreatePartitions` (key 37) and
//! `IncrementalAlterConfigs` (key 44) translate wire-level config
//! changes into PATCH operations on the corresponding `KafkaTopic`
//! CR. The operator then reconciles the change normally — no
//! direct broker → operator coupling.
//!
//! ## Trait + impls
//!
//! The trait lives at the top level so handlers can take an
//! `Arc<dyn TopicCRWriter>` without depending on `kube`. Two impls:
//!
//! - [`KubeTopicCRWriter`] (feature `cr-writer`): real kube-backed
//!   `Patch::Merge` against `Api<KafkaTopic>`.
//! - [`NoopTopicCRWriter`] (always available): the handler returns
//!   `Forbidden` so the wire response is
//!   `CLUSTER_AUTHORIZATION_FAILED` (31). Used in dev mode and
//!   tests.
//!
//! ## Op surface
//!
//! [`ConfigOp`] mirrors Apache's IncrementalAlterConfigs op enum:
//! `Set` and `Delete` map onto JSON-merge patches; `Append` and
//! `Subtract` are list-valued ops that kaas's topic configs
//! don't support — the writer returns [`TopicWriteError::UnsupportedOp`]
//! and the handler surfaces it as `UNSUPPORTED_VERSION` (35).

use async_trait::async_trait;
use serde_json::Value;
use thiserror::Error;

/// One incremental config-key mutation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigOp {
    /// Topic-config key, e.g. `retention.ms`. Mapped onto the
    /// corresponding `KafkaTopic.spec.config.*` JSON field by
    /// [`TopicCRWriter::update_topic_config`].
    pub key: String,
    pub kind: ConfigOpKind,
}

/// `IncrementalAlterConfigs.AlterConfigOp.OpType` enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigOpKind {
    /// Set the key to `value`. `value` is None when the wire payload
    /// is null (Apache uses null for "remove"; clients shouldn't
    /// send a Set op with null, but the codec allows it).
    Set,
    /// Remove the key — patch as JSON null.
    Delete,
    /// Append to a list-valued config. kaas's keys are all
    /// scalar — returns `UnsupportedOp` at the writer.
    Append,
    /// Subtract from a list-valued config. Same as Append.
    Subtract,
}

impl ConfigOp {
    /// Convenience: pair a value with a `Set` op.
    pub fn set(key: impl Into<String>, _value: impl Into<String>) -> Self {
        // Note: value is consumed by [`TopicCRWriter::update_topic_config`]'s
        // patch construction at the impl layer; the public op carries the
        // discriminant only. Callers thread the actual value through
        // a parallel slice. (The Apache wire shape has value alongside
        // op; the handler reads both off the codec request.)
        Self {
            key: key.into(),
            kind: ConfigOpKind::Set,
        }
    }
    pub fn delete(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            kind: ConfigOpKind::Delete,
        }
    }
}

/// Errors a writer can surface. Mapped to wire error codes at the
/// handler boundary — see the per-handler tables.
#[derive(Debug, Error)]
pub enum TopicWriteError {
    /// `KafkaTopic` CR with this name doesn't exist in the operator's
    /// namespace. Wire: `UNKNOWN_TOPIC_OR_PARTITION` (3).
    #[error("topic not found: {0}")]
    NotFound(String),

    /// Patch was refused (RBAC, admission webhook). Wire:
    /// `CLUSTER_AUTHORIZATION_FAILED` (31).
    #[error("forbidden: {0}")]
    Forbidden(String),

    /// Caller tried to use `Append` / `Subtract` on a scalar config
    /// key. Wire: `UNSUPPORTED_VERSION` (35).
    #[error("unsupported config op: {0:?}")]
    UnsupportedOp(ConfigOpKind),

    /// Caller tried to shrink partition count. Wire:
    /// `INVALID_PARTITIONS` (37).
    #[error("invalid partitions: {0}")]
    InvalidPartitions(String),

    /// CreateTopics: a CR with the requested name already exists.
    /// Wire: `TOPIC_ALREADY_EXISTS` (36).
    #[error("topic already exists: {0}")]
    AlreadyExists(String),

    /// Anything else; bubble up for logging. Wire:
    /// `UNKNOWN_SERVER_ERROR` (-1).
    #[error("other: {0}")]
    Other(String),
}

/// Patch operations the handler issues against the CR.
#[async_trait]
pub trait TopicCRWriter: Send + Sync + 'static {
    /// Create a fresh `KafkaTopic` CR. Called by
    /// `CreateTopicsHandler` (API key 19). `Ok(())` on success or
    /// when the CR already exists (idempotent creates map to
    /// `TOPIC_ALREADY_EXISTS` upstream — the caller decides which
    /// error code to surface).
    async fn create_topic(&self, name: &str, num_partitions: i32) -> Result<(), TopicWriteError>;

    /// Patch `KafkaTopic.spec.partitions` to `new_count`. The
    /// operator's reconciler validates the decrease guard; this
    /// helper also catches it client-side so the wire response is
    /// precise.
    async fn expand_topic(&self, name: &str, new_count: i32) -> Result<(), TopicWriteError>;

    /// Apply a set of `(name, op, value)` mutations to
    /// `KafkaTopic.spec.config`. The writer maps each op to a JSON
    /// patch: `Set` → field = parsed-value, `Delete` → field = null.
    /// `Append` / `Subtract` return [`TopicWriteError::UnsupportedOp`].
    async fn update_topic_config(
        &self,
        name: &str,
        ops: &[ConfigOpWithValue],
    ) -> Result<(), TopicWriteError>;

    /// Delete the `KafkaTopic` CR. Called by `DeleteTopicsHandler`
    /// (API key 20). The operator's reconciler tears down the
    /// partition dirs and the topic-watcher fires Deleted on every
    /// broker so open handles close before `remove_dir_all` swings.
    async fn delete_topic(&self, name: &str) -> Result<(), TopicWriteError>;

    /// gh #221 phase 3: flip one partition's placement record
    /// (`status.volumeAssignments[partition] = log_dir`) after an
    /// `AlterReplicaLogDirs` copy completes. The reconciler's
    /// or_insert-only stamping never fights the flipped value.
    async fn set_partition_log_dir(
        &self,
        name: &str,
        partition: i32,
        log_dir: &str,
    ) -> Result<(), TopicWriteError>;
}

/// Op + value pair the handler passes through to the writer. Kept
/// separate from [`ConfigOp`] so the value's lifetime stays scoped
/// to the patch operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigOpWithValue {
    pub key: String,
    pub kind: ConfigOpKind,
    /// `None` ↔ wire null. Always `None` for `Delete`.
    pub value: Option<String>,
}

/// Convert a topic-config key + value string into a JSON value for
/// the spec.config patch. The shape mirrors what the operator's
/// `KafkaTopicConfig` deserialiser expects — integer fields as
/// JSON numbers, `cleanupPolicy` as a string.
pub fn config_value_to_json(key: &str, value: &str) -> Value {
    match key {
        // Integer fields: parse as i64; fall back to string on parse failure
        // so the operator-side schema validation produces a clean error.
        "segment.ms"
        | "segmentMs"
        | "retention.ms"
        | "retentionMs"
        | "retention.bytes"
        | "retentionBytes"
        | "segment.bytes"
        | "segmentBytes"
        | "min.compaction.lag.ms"
        | "minCompactionLagMs"
        | "delete.retention.ms"
        | "deleteRetentionMs" => match value.parse::<i64>() {
            Ok(n) => Value::Number(n.into()),
            Err(_) => Value::String(value.to_string()),
        },
        // Scalar string fields.
        "cleanup.policy" | "cleanupPolicy" => Value::String(value.to_string()),
        // Unknown key: pass through as string and let the operator
        // schema reject it.
        _ => Value::String(value.to_string()),
    }
}

/// Map an Apache wire `key` to the JSON field on
/// `KafkaTopicConfig`. The CR carries camelCase fields; the wire
/// uses dotted names. Returns `None` for unknown keys, which the
/// handler reports as `UNSUPPORTED_VERSION`.
pub fn config_key_to_json_field(key: &str) -> Option<&'static str> {
    match key {
        "retention.ms" | "retentionMs" => Some("retentionMs"),
        "segment.ms" | "segmentMs" => Some("segmentMs"),
        "retention.bytes" | "retentionBytes" => Some("retentionBytes"),
        "segment.bytes" | "segmentBytes" => Some("segmentBytes"),
        "cleanup.policy" | "cleanupPolicy" => Some("cleanupPolicy"),
        "min.compaction.lag.ms" | "minCompactionLagMs" => Some("minCompactionLagMs"),
        "delete.retention.ms" | "deleteRetentionMs" => Some("deleteRetentionMs"),
        _ => None,
    }
}

/// Returns the `(metadata.name, spec.topicName)` pair for a Kafka
/// topic name (gh #86). A valid RFC 1123 subdomain name is used as
/// `metadata.name` directly with `spec.topicName` left empty
/// (Strimzi's recommendation). Otherwise — uppercase Streams
/// internals like `app-KSTREAM-AGGREGATE-...-repartition`, dotted
/// names, >253 chars — synthesise a deterministic
/// `kaas-topic-<16 hex of sha1[:8]>` and stash the literal Kafka
/// name in `spec.topicName`.
///
/// The synthetic shape MUST stay byte-identical to the v0.1 broker's
/// output: during a mixed-version rollout both sides must resolve the
/// same Kafka name to the same CR, or a flavor flip would duplicate
/// CRs for the same topic directory.
pub fn name_for_cr(kafka_name: &str) -> (String, String) {
    if kafka_name.len() <= 253 && is_rfc1123_subdomain(kafka_name) {
        return (kafka_name.to_string(), String::new());
    }
    use sha1::{Digest, Sha1};
    let digest = Sha1::digest(kafka_name.as_bytes());
    use std::fmt::Write;
    let hex = digest[..8].iter().fold(String::new(), |mut acc, b| {
        let _ = write!(acc, "{b:02x}");
        acc
    });
    (format!("kaas-topic-{hex}"), kafka_name.to_string())
}

/// K8s resource-name validation: lowercase alphanumeric labels with
/// interior hyphens, dot-separated. Implements the rfc1123
/// check without pulling the regex crate in.
fn is_rfc1123_subdomain(s: &str) -> bool {
    !s.is_empty()
        && s.split('.').all(|label| {
            !label.is_empty()
                && label.starts_with(|c: char| c.is_ascii_lowercase() || c.is_ascii_digit())
                && label.ends_with(|c: char| c.is_ascii_lowercase() || c.is_ascii_digit())
                && label
                    .chars()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
        })
}

/// Dev-mode writer: every operation returns `Forbidden` so the
/// handler maps to `CLUSTER_AUTHORIZATION_FAILED` (31). The
/// `bins/kaas` main wires this when `MY_POD_NAME` is unset (no
/// kube client to dispatch against).
#[derive(Debug, Default)]
pub struct NoopTopicCRWriter;

#[async_trait]
impl TopicCRWriter for NoopTopicCRWriter {
    async fn create_topic(&self, _name: &str, _num_partitions: i32) -> Result<(), TopicWriteError> {
        Err(TopicWriteError::Forbidden(
            "broker is not running in cluster mode".into(),
        ))
    }
    async fn expand_topic(&self, _name: &str, _new_count: i32) -> Result<(), TopicWriteError> {
        Err(TopicWriteError::Forbidden(
            "broker is not running in cluster mode".into(),
        ))
    }
    async fn update_topic_config(
        &self,
        _name: &str,
        _ops: &[ConfigOpWithValue],
    ) -> Result<(), TopicWriteError> {
        Err(TopicWriteError::Forbidden(
            "broker is not running in cluster mode".into(),
        ))
    }
    async fn delete_topic(&self, _name: &str) -> Result<(), TopicWriteError> {
        Err(TopicWriteError::Forbidden(
            "broker is not running in cluster mode".into(),
        ))
    }
    async fn set_partition_log_dir(
        &self,
        _name: &str,
        _partition: i32,
        _log_dir: &str,
    ) -> Result<(), TopicWriteError> {
        Err(TopicWriteError::Forbidden(
            "broker is not running in cluster mode".into(),
        ))
    }
}

// --- kube-backed impl ------------------------------------------------

#[cfg(feature = "cr-writer")]
pub use kube_impl::KubeTopicCRWriter;

#[cfg(feature = "cr-writer")]
mod kube_impl {
    use super::*;
    use kaas_operator_api::KafkaTopic;
    use kube::api::{Patch, PatchParams};

    /// gh #245: stamp broker writes with a recognisable fieldManager
    /// so managedFields attributes them (default is `unknown`).
    fn broker_patch_params() -> PatchParams {
        PatchParams {
            field_manager: Some("kaas-broker".to_owned()),
            ..Default::default()
        }
    }
    use kube::Api;
    use serde_json::json;

    /// Real kube-backed writer.
    #[derive(Clone)]
    pub struct KubeTopicCRWriter {
        client: kube::Client,
        namespace: String,
    }

    impl std::fmt::Debug for KubeTopicCRWriter {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("KubeTopicCRWriter")
                .field("namespace", &self.namespace)
                .finish_non_exhaustive()
        }
    }

    impl KubeTopicCRWriter {
        pub fn new(client: kube::Client, namespace: impl Into<String>) -> Self {
            Self {
                client,
                namespace: namespace.into(),
            }
        }

        fn api(&self) -> Api<KafkaTopic> {
            Api::namespaced(self.client.clone(), &self.namespace)
        }
    }

    #[async_trait::async_trait]
    impl TopicCRWriter for KubeTopicCRWriter {
        async fn create_topic(
            &self,
            name: &str,
            num_partitions: i32,
        ) -> Result<(), TopicWriteError> {
            use kaas_operator_api::{KafkaTopicConfig, KafkaTopicSpec};
            use kube::api::PostParams;

            // gh #86: non-RFC-1123 Kafka names (Streams internals)
            // get a deterministic synthetic CR name with the literal
            // name carried in spec.topicName.
            let (meta_name, topic_name) = super::name_for_cr(name);
            let cr = KafkaTopic {
                metadata: kube::api::ObjectMeta {
                    name: Some(meta_name),
                    namespace: Some(self.namespace.clone()),
                    ..Default::default()
                },
                spec: KafkaTopicSpec {
                    topic_name,
                    partitions: num_partitions,
                    config: KafkaTopicConfig::default(),
                    storage: None,
                },
                status: None,
            };
            // gh #245: name the writer. Without a fieldManager every
            // broker-minted CR shows `manager: unknown` in
            // managedFields, which is exactly what made the 2026-08-02
            // CR-deletion incident unattributable after the fact.
            let pp = PostParams {
                field_manager: Some("kaas-broker".to_owned()),
                ..Default::default()
            };
            match self.api().create(&pp, &cr).await {
                Ok(_) => Ok(()),
                Err(kube::Error::Api(e)) if e.code == 409 => {
                    Err(TopicWriteError::AlreadyExists(name.into()))
                }
                Err(e) => Err(map_kube_err(e)),
            }
        }

        async fn expand_topic(&self, name: &str, new_count: i32) -> Result<(), TopicWriteError> {
            // Client-side decrease guard: read current, refuse if
            // shrinking. The operator-side reconciler enforces the
            // same rule (with the status-condition message); doing
            // it here too returns a precise wire code without
            // round-tripping the operator.
            let (meta_name, _) = super::name_for_cr(name);
            let api = self.api();
            match api.get(&meta_name).await {
                Ok(t) => {
                    if t.spec.partitions > new_count {
                        return Err(TopicWriteError::InvalidPartitions(format!(
                            "current {} → requested {}",
                            t.spec.partitions, new_count
                        )));
                    }
                }
                Err(kube::Error::Api(e)) if e.code == 404 => {
                    return Err(TopicWriteError::NotFound(name.into()));
                }
                Err(e) => return Err(map_kube_err(e)),
            }
            let patch = json!({ "spec": { "partitions": new_count } });
            api.patch(&meta_name, &broker_patch_params(), &Patch::Merge(&patch))
                .await
                .map(|_| ())
                .map_err(map_kube_err)
        }

        async fn update_topic_config(
            &self,
            name: &str,
            ops: &[ConfigOpWithValue],
        ) -> Result<(), TopicWriteError> {
            let mut config = serde_json::Map::new();
            for op in ops {
                match op.kind {
                    ConfigOpKind::Append | ConfigOpKind::Subtract => {
                        return Err(TopicWriteError::UnsupportedOp(op.kind));
                    }
                    ConfigOpKind::Set => {
                        let Some(field) = config_key_to_json_field(&op.key) else {
                            return Err(TopicWriteError::UnsupportedOp(op.kind));
                        };
                        let Some(value) = op.value.as_deref() else {
                            // Set with null → treat as Delete.
                            config.insert(field.into(), Value::Null);
                            continue;
                        };
                        config.insert(field.into(), config_value_to_json(&op.key, value));
                    }
                    ConfigOpKind::Delete => {
                        let Some(field) = config_key_to_json_field(&op.key) else {
                            return Err(TopicWriteError::UnsupportedOp(op.kind));
                        };
                        config.insert(field.into(), Value::Null);
                    }
                }
            }
            let patch = json!({ "spec": { "config": config } });
            let (meta_name, _) = super::name_for_cr(name);
            let api = self.api();
            match api
                .patch(&meta_name, &broker_patch_params(), &Patch::Merge(&patch))
                .await
            {
                Ok(_) => Ok(()),
                Err(kube::Error::Api(e)) if e.code == 404 => {
                    Err(TopicWriteError::NotFound(name.into()))
                }
                Err(e) => Err(map_kube_err(e)),
            }
        }

        async fn delete_topic(&self, name: &str) -> Result<(), TopicWriteError> {
            use kube::api::DeleteParams;
            let (meta_name, _) = super::name_for_cr(name);
            match self
                .api()
                .delete(&meta_name, &DeleteParams::default())
                .await
            {
                Ok(_) => Ok(()),
                Err(kube::Error::Api(e)) if e.code == 404 => {
                    Err(TopicWriteError::NotFound(name.into()))
                }
                Err(e) => Err(map_kube_err(e)),
            }
        }

        async fn set_partition_log_dir(
            &self,
            name: &str,
            partition: i32,
            log_dir: &str,
        ) -> Result<(), TopicWriteError> {
            // JSON merge patch on the status map: object fields merge,
            // so only this partition's entry changes. Needs the
            // kafkatopics/status RBAC verb (broker-rbac.yaml).
            let mut assignments = serde_json::Map::new();
            assignments.insert(partition.to_string(), Value::String(log_dir.to_owned()));
            let patch = json!({ "status": { "volumeAssignments": assignments } });
            let (meta_name, _) = super::name_for_cr(name);
            match self
                .api()
                .patch_status(&meta_name, &broker_patch_params(), &Patch::Merge(&patch))
                .await
            {
                Ok(_) => Ok(()),
                Err(kube::Error::Api(e)) if e.code == 404 => {
                    Err(TopicWriteError::NotFound(name.into()))
                }
                Err(e) => Err(map_kube_err(e)),
            }
        }
    }

    fn map_kube_err(e: kube::Error) -> TopicWriteError {
        match &e {
            kube::Error::Api(api) if api.code == 403 => {
                TopicWriteError::Forbidden(api.message.clone())
            }
            kube::Error::Api(api) if api.code == 404 => {
                TopicWriteError::NotFound(api.message.clone())
            }
            _ => TopicWriteError::Other(e.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_key_to_json_field_matches_known_keys() {
        assert_eq!(
            config_key_to_json_field("retention.ms"),
            Some("retentionMs")
        );
        assert_eq!(config_key_to_json_field("retentionMs"), Some("retentionMs"));
        assert_eq!(
            config_key_to_json_field("cleanup.policy"),
            Some("cleanupPolicy")
        );
        assert_eq!(
            config_key_to_json_field("min.compaction.lag.ms"),
            Some("minCompactionLagMs")
        );
        assert_eq!(config_key_to_json_field("unknown.key"), None);
    }

    #[test]
    fn config_value_parses_integer_fields() {
        assert_eq!(
            config_value_to_json("retention.ms", "60000"),
            Value::Number(60_000_i64.into())
        );
        assert_eq!(
            config_value_to_json("cleanup.policy", "compact"),
            Value::String("compact".into())
        );
        // Unparseable integer falls back to string.
        assert_eq!(
            config_value_to_json("retention.ms", "huh"),
            Value::String("huh".into())
        );
    }

    #[tokio::test]
    async fn noop_writer_returns_forbidden() {
        let w = NoopTopicCRWriter;
        let err = w.expand_topic("x", 4).await.unwrap_err();
        assert!(matches!(err, TopicWriteError::Forbidden(_)));
        let err = w
            .update_topic_config(
                "x",
                &[ConfigOpWithValue {
                    key: "retention.ms".into(),
                    kind: ConfigOpKind::Set,
                    value: Some("1000".into()),
                }],
            )
            .await
            .unwrap_err();
        assert!(matches!(err, TopicWriteError::Forbidden(_)));
    }
}
