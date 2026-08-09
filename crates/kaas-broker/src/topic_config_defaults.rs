//! Per-key Apache-Kafka-3.7-compatible defaults for the
//! DescribeConfigs handler (key 32).
//!
//! `kafka-configs.sh --describe --entity-type topics` expects every
//! topic config key to be present in the response, with a default
//! value and (v3+) a one-line documentation string. This table is
//! the static answer. Per-topic overrides (the operator-written
//! `.config.json`) wrap it at the broker's next reconcile —
//! the existing `kaas_storage::TopicConfigFile` reader is the
//! source of truth at runtime; this table only describes the
//! cluster-wide defaults a topic falls back to when its file is
//! absent or doesn't carry the key.
//!
//! Values are stringified at the wire boundary; the codec carries
//! `Option<String>`. `config_type` is the Apache `ConfigEntry.Type`
//! discriminant (see [`kaas_codec::api::describe_configs::config_type`]).

use kaas_codec::api::describe_configs::config_type;

/// One row in the defaults table.
#[derive(Debug, Clone, Copy)]
pub struct Entry {
    /// Apache wire name (dotted form).
    pub dotted_name: &'static str,
    /// Default value as a string. `None` ↔ wire null (Apache emits
    /// null for keys whose default is "not set"; e.g.
    /// `message.timestamp.before.max.ms` defaults to `9223372036854775807`,
    /// not null).
    pub default_value: Option<&'static str>,
    /// Apache `ConfigEntry.Type` discriminant.
    pub config_type: i8,
    /// One-line description for v3+ `documentation` field.
    pub documentation: &'static str,
}

/// Subset of Apache's topic-config keys that kaas actually
/// honours (gh #116 cleaner + compactor + retention). Clients gate on these for
/// `--describe` output, so the table is small but load-bearing.
pub const ALL_KEYS: &[Entry] = &[
    Entry {
        dotted_name: "retention.ms",
        default_value: Some("604800000"), // 7 days
        config_type: config_type::LONG,
        documentation: "Maximum time before old log segments are deleted. -1 = retain forever.",
    },
    Entry {
        dotted_name: "retention.bytes",
        default_value: Some("-1"),
        config_type: config_type::LONG,
        documentation:
            "Maximum total bytes per partition before old segments are deleted. -1 = unlimited.",
    },
    Entry {
        dotted_name: "segment.bytes",
        default_value: Some("1073741824"), // 1 GiB
        config_type: config_type::INT,
        documentation: "Bytes at which the active segment rolls.",
    },
    Entry {
        dotted_name: "segment.ms",
        default_value: Some("604800000"), // 7 days
        config_type: config_type::LONG,
        documentation: "Time after which the active segment rolls, even if it is under segment.bytes. -1 = never roll on time.",
    },
    Entry {
        dotted_name: "cleanup.policy",
        default_value: Some("delete"),
        config_type: config_type::LIST,
        documentation:
            "Either 'delete' (size/age retention), 'compact' (log compaction), or 'compact,delete'.",
    },
    Entry {
        dotted_name: "min.compaction.lag.ms",
        default_value: Some("0"),
        config_type: config_type::LONG,
        documentation:
            "Minimum time a message must remain uncompacted (KIP-58). 0 = compact immediately.",
    },
    Entry {
        dotted_name: "delete.retention.ms",
        default_value: Some("86400000"), // 24h
        config_type: config_type::LONG,
        documentation:
            "Tombstone retention for compacted topics (KIP-354). 0 = tombstones live forever.",
    },
];

/// The `retention.ms` default, parsed from the same table
/// DescribeConfigs answers from.
///
/// Read through this rather than hard-coding 7 days at the call site:
/// the advertised default and the enforced default drifting apart is
/// exactly the bug this fixes (kaas advertised 604800000 and enforced
/// nothing). Falls back to Apache's 7 days if the row ever goes
/// missing.
pub fn default_retention_ms() -> u64 {
    lookup("retention.ms")
        .and_then(|e| e.default_value)
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(7 * 24 * 60 * 60 * 1000)
}

/// Convenience: lookup by dotted wire name. Returns `None` for
/// unknown keys; the handler surfaces those as
/// `UNSUPPORTED_VERSION` via the upstream codec path.
pub fn lookup(name: &str) -> Option<&'static Entry> {
    ALL_KEYS.iter().find(|e| e.dotted_name == name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lookup_finds_known_keys() {
        assert_eq!(
            lookup("retention.ms").map(|e| e.config_type),
            Some(config_type::LONG)
        );
        assert!(lookup("does.not.exist").is_none());
    }

    #[test]
    fn every_entry_has_documentation() {
        for e in ALL_KEYS {
            assert!(
                !e.documentation.is_empty(),
                "{} missing docs",
                e.dotted_name
            );
        }
    }

    #[test]
    fn cleanup_policy_default_is_delete() {
        let e = lookup("cleanup.policy").unwrap();
        assert_eq!(e.default_value, Some("delete"));
    }
}
