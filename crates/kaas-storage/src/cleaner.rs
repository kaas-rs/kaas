//! `RetentionCleaner` — size-based log retention.
//!
//! The `delete`-policy cleaner. Walks every owned partition
//! and asks each Partition for its size-based cleanup target; calls
//! `delete_records` when the target is non-trivial.
//!
//! Phase 2 ships size-based retention only. Time-based retention
//! (`retention.ms`) and the compactor with gh #116 `min.compaction.lag.ms`
//! + `delete.retention.ms` knobs are follow-up commits on gh #158.
//!
//! # Threading
//!
//! The cleaner does NOT own a background task — it exposes a
//! [`RetentionCleaner::run_once`] entry point. The broker startup
//! wraps it in a `tokio::time::interval` loop (Phase 3) with a
//! cancellation hook for SIGTERM drain.

use std::sync::Arc;

use crate::disk::DiskStorageEngine;
use crate::errors::StorageError;

/// Per-topic retention policy. For Phase 2 only size-based retention
/// is honoured; time-based and cleanup-policy (`delete` vs `compact`)
/// fields land alongside their respective implementations.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RetentionPolicy {
    /// `retention.bytes`. `None` = no size cap.
    pub retention_bytes: Option<u64>,
}

/// Per-topic policy resolver. The default lookup is a single
/// engine-wide policy (passed to [`RetentionCleaner::new`]); a
/// follow-up commit will read `/data/<topic>/.config.json` via
/// [`crate::topicconfig::read_topic_config`] for per-topic
/// overrides.
pub trait PolicySource: Send + Sync + 'static {
    fn policy_for(&self, topic: &str) -> RetentionPolicy;
}

/// Single-policy source used by the basic `RetentionCleaner::new`
/// constructor.
#[derive(Debug, Clone, Copy)]
pub struct FixedPolicySource {
    policy: RetentionPolicy,
}

impl FixedPolicySource {
    pub fn new(policy: RetentionPolicy) -> Self {
        Self { policy }
    }
}

impl PolicySource for FixedPolicySource {
    fn policy_for(&self, _topic: &str) -> RetentionPolicy {
        self.policy
    }
}

pub struct RetentionCleaner {
    engine: Arc<DiskStorageEngine>,
    policy_source: Arc<dyn PolicySource>,
}

impl std::fmt::Debug for RetentionCleaner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RetentionCleaner")
            .field("engine", &self.engine)
            .finish()
    }
}

impl RetentionCleaner {
    /// Build a cleaner with a single engine-wide retention policy.
    /// Use [`Self::with_policy_source`] for per-topic policies.
    pub fn new(engine: Arc<DiskStorageEngine>, policy: RetentionPolicy) -> Self {
        Self {
            engine,
            policy_source: Arc::new(FixedPolicySource::new(policy)),
        }
    }

    pub fn with_policy_source(
        engine: Arc<DiskStorageEngine>,
        policy_source: Arc<dyn PolicySource>,
    ) -> Self {
        Self {
            engine,
            policy_source,
        }
    }

    /// One cleanup pass over every open partition. Returns the count
    /// of partitions where retention actually triggered a
    /// `delete_records` call (useful for tests + OTel meter wire-up).
    pub async fn run_once(&self) -> Result<u32, StorageError> {
        let started = std::time::Instant::now();
        let out = self.run_once_inner().await;
        let m = kaas_observability::metrics::global();
        m.cleaner_duration
            .record(started.elapsed().as_secs_f64(), &[]);
        m.cleaner_runs.add(
            1,
            &[kaas_observability::KeyValue::new(
                "result",
                if out.is_ok() { "ok" } else { "error" },
            )],
        );
        out
    }

    async fn run_once_inner(&self) -> Result<u32, StorageError> {
        let mut cleaned = 0u32;
        for (topic, partition) in self.engine.iter_partition_keys() {
            let Some(p) = self.engine.partition(&topic, partition) else {
                continue;
            };
            let policy = self.policy_source.policy_for(&topic);
            let Some(retention_bytes) = policy.retention_bytes else {
                continue;
            };
            let Some(target) = p.cleanup_target_for_size_bytes(retention_bytes) else {
                continue;
            };
            // delete_records is idempotent under "target <= log_start".
            // Skip the call when the target would be a no-op.
            if target <= p.log_start_offset() {
                continue;
            }
            let before = p.partition_size();
            p.delete_records(target).await?;
            let after = p.partition_size();
            let reclaimed = before.saturating_sub(after);
            cleaned += 1;
            let m = kaas_observability::metrics::global();
            m.cleaner_segments_deleted
                .add(1, &[kaas_observability::KeyValue::new("reason", "size")]);
            if reclaimed > 0 {
                m.cleaner_bytes_reclaimed.add(
                    u64::try_from(reclaimed).unwrap_or(0),
                    &[kaas_observability::KeyValue::new("reason", "size")],
                );
            }
        }
        Ok(cleaned)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::StorageEngine;
    use crate::fs::{Fs, RealFs};
    use crate::partition::PartitionConfig;
    use bytes::Bytes;
    use std::path::PathBuf;

    fn rt() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap()
    }

    fn build_batch(num_records: i32, max_timestamp: i64) -> Bytes {
        let body_size = 49 + 16;
        let total = 12 + body_size;
        let mut buf = vec![0u8; total];
        buf[0..8].copy_from_slice(&0i64.to_be_bytes());
        let body_len_i32 = i32::try_from(body_size).unwrap();
        buf[8..12].copy_from_slice(&body_len_i32.to_be_bytes());
        buf[16] = 2;
        let last_offset_delta = num_records - 1;
        buf[23..27].copy_from_slice(&last_offset_delta.to_be_bytes());
        buf[35..43].copy_from_slice(&max_timestamp.to_be_bytes());
        buf[43..51].copy_from_slice(&(-1i64).to_be_bytes());
        crate::segment::stamp_crc(&mut buf);
        Bytes::from(buf)
    }

    fn engine_at(dir: PathBuf, segment_bytes: u64) -> Arc<DiskStorageEngine> {
        let fs: Arc<dyn Fs> = Arc::new(RealFs::new());
        Arc::new(DiskStorageEngine::new(
            fs,
            dir,
            PartitionConfig {
                segment_bytes,
                ..Default::default()
            },
        ))
    }

    #[test]
    fn no_op_when_total_under_cap() {
        let tmp = tempfile::tempdir().unwrap();
        rt().block_on(async {
            let e = engine_at(tmp.path().to_path_buf(), 1 << 30);
            for _ in 0..3 {
                e.append("t", 0, 0, -1, build_batch(1, 1_000))
                    .await
                    .unwrap();
            }
            let cleaner = RetentionCleaner::new(
                e.clone(),
                RetentionPolicy {
                    retention_bytes: Some(1 << 30),
                },
            );
            let cleaned = cleaner.run_once().await.unwrap();
            assert_eq!(cleaned, 0);
            assert_eq!(e.log_start_offset("t", 0).unwrap(), 0);
            e.relinquish_all().await.unwrap();
        });
    }

    #[test]
    fn drops_oldest_closed_segments_when_over_cap() {
        let tmp = tempfile::tempdir().unwrap();
        rt().block_on(async {
            // Roll after each batch — each closed segment is exactly
            // one batch worth.
            let one_len = u64::try_from(build_batch(1, 1_000).len()).unwrap();
            let e = engine_at(tmp.path().to_path_buf(), one_len);
            // 5 appends → at least 4 closed segments + 1 active.
            for _ in 0..5 {
                e.append("t", 0, 0, -1, build_batch(1, 1_000))
                    .await
                    .unwrap();
            }
            // Keep only the last batch's worth of data.
            let cleaner = RetentionCleaner::new(
                e.clone(),
                RetentionPolicy {
                    retention_bytes: Some(one_len),
                },
            );
            let cleaned = cleaner.run_once().await.unwrap();
            assert_eq!(cleaned, 1, "exactly one partition was cleaned");
            // log_start should have advanced past at least 3 records.
            assert!(
                e.log_start_offset("t", 0).unwrap() >= 3,
                "expected log_start to advance past dropped batches"
            );
            e.relinquish_all().await.unwrap();
        });
    }

    #[test]
    fn no_policy_means_no_cleanup() {
        let tmp = tempfile::tempdir().unwrap();
        rt().block_on(async {
            let one_len = u64::try_from(build_batch(1, 1_000).len()).unwrap();
            let e = engine_at(tmp.path().to_path_buf(), one_len);
            for _ in 0..5 {
                e.append("t", 0, 0, -1, build_batch(1, 1_000))
                    .await
                    .unwrap();
            }
            let cleaner = RetentionCleaner::new(
                e.clone(),
                RetentionPolicy {
                    retention_bytes: None,
                },
            );
            let cleaned = cleaner.run_once().await.unwrap();
            assert_eq!(cleaned, 0);
            assert_eq!(e.log_start_offset("t", 0).unwrap(), 0);
            e.relinquish_all().await.unwrap();
        });
    }

    #[test]
    fn cleaner_is_idempotent_under_repeat_passes() {
        let tmp = tempfile::tempdir().unwrap();
        rt().block_on(async {
            let one_len = u64::try_from(build_batch(1, 1_000).len()).unwrap();
            let e = engine_at(tmp.path().to_path_buf(), one_len);
            for _ in 0..5 {
                e.append("t", 0, 0, -1, build_batch(1, 1_000))
                    .await
                    .unwrap();
            }
            let cleaner = RetentionCleaner::new(
                e.clone(),
                RetentionPolicy {
                    retention_bytes: Some(one_len * 2),
                },
            );
            // First pass cleans; subsequent passes are no-ops.
            let a = cleaner.run_once().await.unwrap();
            let b = cleaner.run_once().await.unwrap();
            let c = cleaner.run_once().await.unwrap();
            assert_eq!(a, 1);
            assert_eq!(b, 0);
            assert_eq!(c, 0);
            e.relinquish_all().await.unwrap();
        });
    }

    #[test]
    fn cleanup_target_lock_free_calculation() {
        // Direct test of Partition::cleanup_target_for_size_bytes
        // outside the cleaner orchestration.
        let tmp = tempfile::tempdir().unwrap();
        rt().block_on(async {
            let one_len = u64::try_from(build_batch(1, 1_000).len()).unwrap();
            let e = engine_at(tmp.path().to_path_buf(), one_len);
            for _ in 0..4 {
                e.append("t", 0, 0, -1, build_batch(1, 1_000))
                    .await
                    .unwrap();
            }
            let p = e.partition("t", 0).unwrap();
            // Three closed segments; active has the last record.
            // retention_bytes = one_len * 2 → keep ~2 segments worth.
            let target = p.cleanup_target_for_size_bytes(one_len * 2);
            assert!(target.is_some(), "expected a non-None cleanup target");
            // No cleanup when retention is huge.
            assert!(p.cleanup_target_for_size_bytes(1 << 30).is_none());
            e.relinquish_all().await.unwrap();
        });
    }
}
