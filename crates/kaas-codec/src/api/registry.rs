//! Single declarative table of every API key the broker exposes.
//!
//! Drives the ApiVersions response in [`crate::api::api_versions`] and the
//! header-version lookup in [`crate::headers`]. The full 40-key table is
//! filled in over the course of Phase 1 — each per-API module's commit
//! adds its row here.

use crate::headers::HeaderVersion;

// No `PartialEq`/`Eq`: the two `fn` pointer fields below make derived
// equality meaningless (function addresses aren't unique across codegen
// units), and rustc denies the comparison since 1.86. Nothing compares
// `ApiSpec` values — lookups go by `key`.
#[derive(Debug, Clone, Copy)]
pub struct ApiSpec {
    pub key: i16,
    pub min_version: i16,
    pub max_version: i16,
    /// `Some(v)` if the API is flexible (KIP-482) from version `v` onward;
    /// `None` if all supported versions are still legacy.
    pub min_flexible: Option<i16>,
    pub request_hdr: fn(i16) -> HeaderVersion,
    pub response_hdr: fn(i16) -> HeaderVersion,
}

impl ApiSpec {
    /// True if `version` is in the flexible range for this API.
    pub fn is_flexible(&self, version: i16) -> bool {
        matches!(self.min_flexible, Some(min) if version >= min)
    }
}

/// Every API key the broker registers, with its supported version
/// range. One entry per per-API module.
pub const ALL: &[ApiSpec] = &[
    crate::api::produce::SPEC,
    crate::api::fetch::SPEC,
    crate::api::list_offsets::SPEC,
    crate::api::metadata::SPEC,
    crate::api::offset_commit::SPEC,
    crate::api::offset_fetch::SPEC,
    crate::api::find_coordinator::SPEC,
    crate::api::join_group::SPEC,
    crate::api::heartbeat::SPEC,
    crate::api::leave_group::SPEC,
    crate::api::sync_group::SPEC,
    crate::api::describe_groups::SPEC,
    crate::api::list_groups::SPEC,
    crate::api::sasl_handshake::SPEC,
    crate::api::init_producer_id::SPEC,
    crate::api::add_partitions_to_txn::SPEC,
    crate::api::add_offsets_to_txn::SPEC,
    crate::api::end_txn::SPEC,
    crate::api::write_txn_markers::SPEC,
    crate::api::txn_offset_commit::SPEC,
    crate::api::sasl_authenticate::SPEC,
    crate::api::api_versions::SPEC,
    crate::api::delete_groups::SPEC,
    crate::api::offset_delete::SPEC,
    // Phase 7 admin surface (workstream D)
    crate::api::describe_configs::SPEC,
    crate::api::create_partitions::SPEC,
    crate::api::create_topics::SPEC,
    crate::api::incremental_alter_configs::SPEC,
    crate::api::describe_client_quotas::SPEC,
    crate::api::alter_client_quotas::SPEC,
    // Admin surface (gh #152)
    crate::api::delete_topics::SPEC,
    crate::api::delete_records::SPEC,
    crate::api::describe_acls::SPEC,
    crate::api::create_acls::SPEC,
    crate::api::delete_acls::SPEC,
    crate::api::alter_replica_log_dirs::SPEC,
    crate::api::describe_log_dirs::SPEC,
];

/// Look up the [`ApiSpec`] for a given API key, if registered.
pub fn lookup(key: i16) -> Option<&'static ApiSpec> {
    ALL.iter().find(|s| s.key == key)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lookup_returns_known_key() {
        let spec = lookup(18).expect("ApiVersions seeded in ALL");
        assert_eq!(spec.key, 18);
        assert_eq!(spec.min_version, 0);
        assert_eq!(spec.max_version, 4);
        assert_eq!(spec.min_flexible, Some(3));
    }

    #[test]
    fn lookup_returns_none_for_unknown_key() {
        assert!(lookup(999).is_none());
    }

    #[test]
    fn flex_predicate() {
        let spec = lookup(18).expect("seeded");
        assert!(!spec.is_flexible(2));
        assert!(spec.is_flexible(3));
        assert!(spec.is_flexible(4));
    }

    /// Phase 6 exit criterion §A — registry pins to 24 entries:
    /// keys 0, 1, 2, 3, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 22,
    /// 24, 25, 26, 27, 28, 36, 42, 47. Bump this number when a new
    /// module lands.
    #[test]
    fn registry_size_phase7() {
        // Phase 7 workstream D added keys 32, 37, 44, 48, 49 — the
        // admin surface (Describe/AlterConfigs, CreatePartitions,
        // Describe/AlterClientQuotas). Phase 8 workstream C added
        // key 19 (CreateTopics) once the scripts smoke started
        // needing it. gh #152 added the
        // admin keys 20, 21, 29, 30, 31, 35 (DeleteTopics,
        // DeleteRecords, the ACL trio, DescribeLogDirs).
        assert_eq!(ALL.len(), 37);
        let mut keys: Vec<i16> = ALL.iter().map(|s| s.key).collect();
        keys.sort_unstable();
        assert_eq!(
            keys,
            vec![
                0, 1, 2, 3, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 24, 25, 26,
                27, 28, 29, 30, 31, 32, 34, 35, 36, 37, 42, 44, 47, 48, 49,
            ]
        );
    }
}
