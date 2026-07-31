//! `TakeoverDriver` — reacts to assignment changes by driving the
//! storage engine through `take_over` / `relinquish`.
//!
//! Registered as an
//! [`AssignmentChangeHandler`] on the [`crate::coordinator::Coordinator`];
//! runs synchronously on the watcher task. The driver does *not*
//! perform recovery itself — it dispatches per-partition
//! `take_over(topic, partition, epoch)` / `relinquish(topic,
//! partition)` calls into the [`StorageEngine`].
//!
//! [`AssignmentChangeHandler`]: crate::assignment::AssignmentChangeHandler

use std::collections::HashMap;
use std::sync::Arc;

use kaas_storage::StorageEngine;
use tracing::warn;

use crate::assignment::Assignment;
use crate::coordinator::partition_key;

/// Per-`(topic, partition)` reference carried across the prev→next
/// diff. Internal to the driver.
#[derive(Debug, Clone)]
struct OwnedRef {
    topic: String,
    partition: i32,
    epoch: u32,
}

/// Bound to a [`StorageEngine`] + the broker's own ID; the driver
/// itself is stateless. The Coordinator owns one `Arc<TakeoverDriver>`
/// and registers it via `on_assignment_change`.
pub struct TakeoverDriver {
    engine: Arc<dyn StorageEngine>,
    broker_id: String,
}

impl std::fmt::Debug for TakeoverDriver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TakeoverDriver")
            .field("broker_id", &self.broker_id)
            .finish_non_exhaustive()
    }
}

impl TakeoverDriver {
    pub fn new(engine: Arc<dyn StorageEngine>, broker_id: impl Into<String>) -> Arc<Self> {
        Arc::new(Self {
            engine,
            broker_id: broker_id.into(),
        })
    }

    /// Build the [`AssignmentChangeHandler`] closure for this driver.
    /// Register it with `coordinator.on_assignment_change(...)` at
    /// boot.
    pub fn as_handler(self: &Arc<Self>) -> crate::assignment::AssignmentChangeHandler {
        let me = self.clone();
        Arc::new(move |prev, next| me.on_change(prev, next))
    }

    /// Diff `prev` vs `next.partitions` and dispatch
    /// take-over / relinquish calls. The storage methods are async;
    /// each is spawned as a `tokio::task` so the watcher can move on
    /// to the next tick — Apache's contract is "the next heartbeat
    /// reports RECOVERING / ERROR if recovery is slow", which we
    /// preserve by surfacing errors via the storage engine's
    /// per-partition state rather than blocking here.
    pub fn on_change(&self, prev: Option<&Assignment>, next: &Assignment) {
        let prev_owned = self.owned_by(prev);
        let next_owned = self.owned_by(Some(next));

        for (k, ne) in &next_owned {
            let needs_takeover = match prev_owned.get(k) {
                Some(pe) => pe.epoch != ne.epoch,
                None => true,
            };
            if !needs_takeover {
                continue;
            }
            let engine = self.engine.clone();
            let topic = ne.topic.clone();
            let partition = ne.partition;
            let epoch = ne.epoch;
            tokio::spawn(async move {
                if let Err(e) = engine.take_over(&topic, partition, epoch).await {
                    warn!(
                        topic = topic.as_str(),
                        partition,
                        epoch,
                        %e,
                        "take_over failed"
                    );
                }
            });
        }

        for (k, pe) in &prev_owned {
            if next_owned.contains_key(k) {
                continue;
            }
            let engine = self.engine.clone();
            let topic = pe.topic.clone();
            let partition = pe.partition;
            tokio::spawn(async move {
                if let Err(e) = engine.relinquish(&topic, partition).await {
                    warn!(
                        topic = topic.as_str(),
                        partition,
                        %e,
                        "relinquish failed"
                    );
                }
            });
        }
    }

    /// gh #215: re-drive `take_over` for every partition the current
    /// assignment gives this broker that the engine has not opened —
    /// **or has open at an epoch below the assignment's** (gh #227).
    ///
    /// [`on_change`] only fires `take_over` when a partition's epoch
    /// changes, so a transient failure — the gh #203 ENOENT race, a
    /// slow-storage blip — is never retried: the partition stays
    /// unopened until a restart or the next epoch bump. With honest
    /// readiness (gh #208) that leaves the broker NotReady and stalls
    /// the rollout.
    ///
    /// The epoch half of the check exists because `take_over` no longer
    /// fails only while opening. Since gh #227 it also rolls a segment
    /// and persists the manifest *after* the open, so a failure there
    /// leaves a partition that is open — and therefore counted as
    /// converged — while still sitting at the previous leader's epoch,
    /// with the append fence unarmed. "Open" and "open at the right
    /// epoch" are different questions, and only the second one means
    /// takeover actually finished.
    ///
    /// This is the NFS-substrate rule-2 backstop (see
    /// `docs/src/architecture/nfs-substrate.md`): `take_over` is
    /// idempotent — a no-op at or below the partition's current epoch —
    /// so re-driving it on a timer converges the engine to the
    /// assignment without ever double-opening or re-rolling. Returns the
    /// number of partitions re-driven this pass.
    ///
    /// [`on_change`]: Self::on_change
    pub fn reconcile(&self, next: &Assignment) -> usize {
        let owned = self.owned_by(Some(next));
        let open: std::collections::HashSet<(String, i32)> =
            self.engine.open_partition_keys().into_iter().collect();
        let mut redriven = 0;
        for oref in owned.values() {
            let key = (oref.topic.clone(), oref.partition);
            if open.contains(&key) {
                // Open at or above the assignment's epoch — converged.
                // An engine that reports no epoch (the memory/dev
                // engines) keeps the old open-set-only semantics.
                let stale = self
                    .engine
                    .partition_epoch(&oref.topic, oref.partition)
                    .is_some_and(|have| have < i64::from(oref.epoch));
                if !stale {
                    continue;
                }
            }
            let engine = self.engine.clone();
            let topic = oref.topic.clone();
            let partition = oref.partition;
            let epoch = oref.epoch;
            redriven += 1;
            tokio::spawn(async move {
                if let Err(e) = engine.take_over(&topic, partition, epoch).await {
                    warn!(
                        topic = topic.as_str(),
                        partition,
                        epoch,
                        %e,
                        "reconcile take_over failed"
                    );
                }
            });
        }
        redriven
    }

    fn owned_by(&self, a: Option<&Assignment>) -> HashMap<String, OwnedRef> {
        let mut out = HashMap::new();
        let a = match a {
            None => return out,
            Some(a) => a,
        };
        for p in &a.partitions {
            if p.broker != self.broker_id {
                continue;
            }
            out.insert(
                partition_key(&p.topic, p.partition),
                OwnedRef {
                    topic: p.topic.clone(),
                    partition: p.partition,
                    epoch: p.epoch,
                },
            );
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::assignment::{BrokerAssignment, BrokerHealth, PartitionAssignment, PartitionRole};
    use kaas_storage::MemoryStorage;

    fn assignment(self_id: &str, parts: &[(&str, i32)]) -> Assignment {
        Assignment {
            controller_epoch: 1,
            assignment_version: 1,
            generated_at: String::new(),
            controller: self_id.to_owned(),
            brokers: vec![BrokerAssignment {
                id: self_id.to_owned(),
                health: BrokerHealth::Alive,
                last_seen: String::new(),
            }],
            partitions: parts
                .iter()
                .map(|(t, p)| PartitionAssignment {
                    topic: (*t).to_owned(),
                    partition: *p,
                    broker: self_id.to_owned(),
                    epoch: 5,
                    role: PartitionRole::Leader,
                })
                .collect(),
            consumer_groups: Vec::new(),
        }
    }

    /// gh #215: reconcile re-drives take_over for assigned-but-unopened
    /// partitions, and only those — an already-open one is skipped.
    #[tokio::test]
    async fn reconcile_redrives_only_unopened_owned_partitions() {
        let engine = Arc::new(MemoryStorage::new());
        let driver = TakeoverDriver::new(engine.clone(), "kaas-0");

        // Pre-open t/0 so it must NOT be re-driven.
        engine.take_over("t", 0, 5).await.unwrap();

        let a = assignment("kaas-0", &[("t", 0), ("t", 1), ("t", 2)]);
        // t/1 and t/2 are owned but unopened → 2 re-driven.
        assert_eq!(driver.reconcile(&a), 2);

        // Let the spawned take_overs run; the open set should converge.
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }
        let open: std::collections::HashSet<(String, i32)> =
            engine.open_partition_keys().into_iter().collect();
        assert!(open.contains(&("t".to_owned(), 1)));
        assert!(open.contains(&("t".to_owned(), 2)));

        // Second pass: everything is open now → nothing re-driven.
        assert_eq!(driver.reconcile(&a), 0);
    }

    /// gh #227: a partition can be *open* and still not have finished
    /// taking over — `take_over` rolls a segment and persists the
    /// manifest after the open, and a failure there leaves the partition
    /// serving at the previous leader's epoch with the append fence
    /// unarmed. The open-set check alone calls that converged and
    /// strands it until the next assignment change, so reconcile has to
    /// compare epochs too.
    // Multi-thread: `Partition::take_over` does its roll + persist on
    // `spawn_blocking`, which a current-thread runtime won't drive from
    // `yield_now` alone.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn reconcile_redrives_a_partition_open_at_a_stale_epoch() {
        let tmp = tempfile::tempdir().unwrap();
        let engine = Arc::new(kaas_storage::DiskStorageEngine::new(
            Arc::new(kaas_storage::RealFs::new()),
            tmp.path().to_path_buf(),
            kaas_storage::PartitionConfig::default(),
        ));
        let driver = TakeoverDriver::new(engine.clone(), "kaas-0");

        // Open t/0 at epoch 5 — the assignment below also says 5.
        engine.take_over("t", 0, 5).await.unwrap();
        let at_5 = assignment("kaas-0", &[("t", 0)]);
        assert_eq!(
            driver.reconcile(&at_5),
            0,
            "open at the assignment's epoch is converged"
        );

        // The controller moves it to a higher epoch. The partition is
        // still open, so the open-set check alone would skip it.
        let mut at_9 = assignment("kaas-0", &[("t", 0)]);
        at_9.partitions[0].epoch = 9;
        assert_eq!(
            driver.reconcile(&at_9),
            1,
            "open at a stale epoch must be re-driven"
        );

        for _ in 0..200 {
            if engine.partition_epoch("t", 0) == Some(9) {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert_eq!(engine.partition_epoch("t", 0), Some(9));
        // Converged now — and re-driving is idempotent, not a re-roll.
        assert_eq!(driver.reconcile(&at_9), 0);
    }

    /// A partition assigned to a DIFFERENT broker is never re-driven.
    #[tokio::test]
    async fn reconcile_ignores_partitions_owned_by_peers() {
        let engine = Arc::new(MemoryStorage::new());
        let driver = TakeoverDriver::new(engine.clone(), "kaas-0");
        let mut a = assignment("kaas-0", &[("t", 0)]);
        // Add a partition owned by kaas-1.
        a.partitions.push(PartitionAssignment {
            topic: "t".to_owned(),
            partition: 1,
            broker: "kaas-1".to_owned(),
            epoch: 5,
            role: PartitionRole::Leader,
        });
        // Only t/0 (ours, unopened) is re-driven; t/1 (peer's) is not.
        assert_eq!(driver.reconcile(&a), 1);
    }
}
