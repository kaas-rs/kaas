#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

//! gh #250 — retention end to end, composed the way `bins/kaas` wires
//! it at boot.
//!
//! The unit tests in `kaas-storage::cleaner` cover each piece in
//! isolation. What this file covers is the thing that was actually
//! broken for the life of the project: the pieces existed, were tested,
//! and were **never assembled**. `RetentionCleaner` had no constructor
//! call anywhere outside its own tests, so `retention.ms` was accepted
//! by the CRD, echoed back by DescribeConfigs, and enforced by nothing.
//!
//! So these tests build the same composition `main.rs` does — disk
//! engine → `TopicConfigPolicySource` over a real `.config.json` →
//! cleaner with a leadership gate — and assert the log actually shrinks.

use std::sync::Arc;

use bytes::Bytes;
use kaas_storage::{
    DiskStorageEngine, OwnershipSource, PartitionConfig, RealFs, RetentionCleaner, RetentionPolicy,
    StorageEngine, TopicConfigFile, TopicConfigPolicySource,
};

/// Minimal v2 record batch with a CreateTime of `timestamp_ms`.
/// Mirrors the fixture the storage crate's own tests use — the broker
/// never looks inside a batch body (byte opacity), so a header-shaped
/// buffer is a faithful stand-in for a produced record.
fn build_batch(timestamp_ms: i64) -> Bytes {
    let body_size = 49 + 16;
    let total = 12 + body_size;
    let mut buf = vec![0u8; total];
    buf[0..8].copy_from_slice(&0i64.to_be_bytes());
    buf[8..12].copy_from_slice(&i32::try_from(body_size).unwrap().to_be_bytes());
    buf[16] = 2; // magic
    buf[23..27].copy_from_slice(&0i32.to_be_bytes()); // last_offset_delta
    buf[35..43].copy_from_slice(&timestamp_ms.to_be_bytes()); // max_timestamp
    buf[43..51].copy_from_slice(&(-1i64).to_be_bytes());
    kaas_storage::segment::stamp_crc(&mut buf);
    Bytes::from(buf)
}

fn engine_at(dir: std::path::PathBuf, segment_bytes: u64) -> Arc<DiskStorageEngine> {
    Arc::new(DiskStorageEngine::new(
        Arc::new(RealFs),
        dir,
        PartitionConfig {
            segment_bytes,
            ..Default::default()
        },
    ))
}

/// Write the topic config the operator materialises from a
/// `KafkaTopic` CR's `spec.config`.
fn write_config(engine: &Arc<DiskStorageEngine>, topic: &str, cfg: TopicConfigFile) {
    let dir = engine.topic_dir(topic, 0);
    std::fs::create_dir_all(&dir).unwrap();
    kaas_storage::write_topic_config(engine.fs(), &dir, &cfg).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retention_ms_from_topic_config_reclaims_the_log() {
    let tmp = tempfile::tempdir().unwrap();
    let one = u64::try_from(build_batch(0).len()).unwrap();
    let engine = engine_at(tmp.path().to_path_buf(), one);

    let stamped_at = 1_700_000_000_000i64;
    for _ in 0..6 {
        engine
            .append("orders", 0, 0, -1, build_batch(stamped_at))
            .await
            .unwrap();
    }
    assert_eq!(engine.log_start_offset("orders", 0).unwrap(), 0);

    // 24 h retention, exactly as `KafkaTopic.spec.config.retentionMs`
    // reaches the broker.
    write_config(
        &engine,
        "orders",
        TopicConfigFile {
            retention_ms: Some(86_400_000),
            ..Default::default()
        },
    );

    // Two days later.
    let now = stamped_at + 2 * 86_400_000;
    let cleaner = RetentionCleaner::with_policy_source(
        engine.clone(),
        Arc::new(TopicConfigPolicySource::new(
            engine.clone(),
            RetentionPolicy::default(),
        )),
    )
    .with_clock(Arc::new(move || now));

    assert_eq!(cleaner.run_once().await.unwrap(), 1);
    let start = engine.log_start_offset("orders", 0).unwrap();
    assert!(
        start > 0,
        "retention.ms from .config.json did not advance the log start offset"
    );

    // Idempotent: a second pass over an already-clean log is a no-op,
    // so a 5-minute sweep isn't rewriting state forever.
    assert_eq!(cleaner.run_once().await.unwrap(), 0);
    assert_eq!(engine.log_start_offset("orders", 0).unwrap(), start);

    engine.relinquish_all().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_topic_with_no_config_is_retained_forever() {
    // The pre-gh #250 behaviour for every topic, and still the right
    // answer for a topic whose CR sets no retention: absent config must
    // never mean "reap it".
    let tmp = tempfile::tempdir().unwrap();
    let one = u64::try_from(build_batch(0).len()).unwrap();
    let engine = engine_at(tmp.path().to_path_buf(), one);
    for _ in 0..6 {
        engine
            .append("keepme", 0, 0, -1, build_batch(1))
            .await
            .unwrap();
    }

    let cleaner = RetentionCleaner::with_policy_source(
        engine.clone(),
        Arc::new(TopicConfigPolicySource::new(
            engine.clone(),
            RetentionPolicy::default(),
        )),
    )
    .with_clock(Arc::new(|| i64::MAX / 2));

    assert_eq!(cleaner.run_once().await.unwrap(), 0);
    assert_eq!(engine.log_start_offset("keepme", 0).unwrap(), 0);
    engine.relinquish_all().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_follower_never_reaps_the_leaders_segments() {
    // The gate `main.rs` installs, exercised through the same
    // composition. Unlinking a segment a peer leads is NFS substrate
    // rule 3, and unlinking one it holds open silly-renames the file
    // to `.nfsXXXX` instead of freeing the space (gh #76).
    struct Follower;
    impl OwnershipSource for Follower {
        fn owns_partition(&self, _topic: &str, _partition: i32) -> bool {
            false
        }
    }

    let tmp = tempfile::tempdir().unwrap();
    let one = u64::try_from(build_batch(0).len()).unwrap();
    let engine = engine_at(tmp.path().to_path_buf(), one);
    for _ in 0..6 {
        engine
            .append("shared", 0, 0, -1, build_batch(1))
            .await
            .unwrap();
    }
    write_config(
        &engine,
        "shared",
        TopicConfigFile {
            retention_ms: Some(1),
            ..Default::default()
        },
    );

    let cleaner = RetentionCleaner::with_policy_source(
        engine.clone(),
        Arc::new(TopicConfigPolicySource::new(
            engine.clone(),
            RetentionPolicy::default(),
        )),
    )
    .with_clock(Arc::new(|| i64::MAX / 2))
    .with_ownership(Arc::new(Follower));

    assert_eq!(cleaner.run_once().await.unwrap(), 0);
    assert_eq!(engine.log_start_offset("shared", 0).unwrap(), 0);
    engine.relinquish_all().await.unwrap();
}
