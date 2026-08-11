//! Broker-side `Coordinator` — assignment.json watcher + ownership
//! lookup + group/txn source.
//!
//! The Coordinator does three things on the hot path:
//!
//! 1. **Read** the most recent `Assignment` (atomic-swapped via
//!    [`ArcSwap`] so produce / fetch never block on a watcher tick).
//! 2. **Validate** new files against the controller-Lease epoch (the
//!    [`LeaseEpochSource`] seam) and reject anything with a stale
//!    epoch — that's the gh #75 / stale-controller-race fence.
//! 3. **Dispatch** [`AssignmentChangeHandler`]s on every successful
//!    apply. The takeover drivers ([`crate::takeover::TakeoverDriver`]
//!    and [`crate::group_takeover::GroupTakeoverDriver`]) are
//!    registered here at boot.
//!
//! In addition it satisfies
//! [`kaas_coordinator::GroupAssignmentSource`] and
//! [`kaas_coordinator::TxnAssignmentSource`] so the `coordinator::Manager`
//! consults this same struct via the gh #92 / gh #91 hot-swap
//! mechanism.
//!
//! The kube-backed [`LeaseEpochSource`] (the real
//! `ControllerWatch`) lands in workstream E alongside the rest of
//! the K8s plumbing; the trait keeps the Coordinator runnable in dev
//! mode against a [`LocalLeaseEpoch`] stub.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwapOption;
use parking_lot::Mutex;
use tracing::{debug, warn};

use crate::assignment::{Assignment, AssignmentChangeHandler};
use crate::group_hash::{pick_group_coordinator, pick_txn_coordinator};
use kaas_coordinator::{BrokerId, GroupAssignmentSource, TxnAssignmentSource};

/// 1 s mtime / fsnotify poll cadence (safety net).
const POLL_INTERVAL: Duration = Duration::from_secs(1);

/// "What is the controller's current Lease epoch?" — the fence used
/// to reject writes from a partitioned ex-controller. Production
/// satisfies this from the kube Lease informer (workstream E);
/// dev / tests use [`LocalLeaseEpoch`].
pub trait LeaseEpochSource: Send + Sync + 'static {
    /// Most recent `leaseTransitions` value observed for the
    /// `kaas-controller` Lease. Files with a smaller
    /// `controller_epoch` are rejected by [`Coordinator::apply_if_new`].
    fn current_epoch(&self) -> i64;
}

/// Bootstrap / dev-mode [`LeaseEpochSource`] that always reports
/// epoch `0`. Production swaps in the kube-backed `ControllerWatch`
/// from `kaas-k8s` (workstream E).
#[derive(Debug, Default)]
pub struct LocalLeaseEpoch;

impl LeaseEpochSource for LocalLeaseEpoch {
    fn current_epoch(&self) -> i64 {
        0
    }
}

/// "When did we last hear from the controller?" — used by
/// `is_heartbeat_fresh`. Production satisfies this from the
/// tonic-built [`HeartbeatClient`]; dev returns `None`.
///
/// [`HeartbeatClient`]: <Phase-5 §C heartbeat_client TODO>
pub trait HeartbeatSource: Send + Sync + 'static {
    fn last_received(&self) -> Option<tokio::time::Instant>;
}

#[derive(Debug, Default)]
pub struct LocalHeartbeat;

impl HeartbeatSource for LocalHeartbeat {
    fn last_received(&self) -> Option<tokio::time::Instant> {
        None
    }
}

/// Most recently applied assignment — guarded by [`ArcSwapOption`]
/// for lock-free reads, with the side `Inner` carrying derived
/// indices.
#[derive(Debug, Default)]
struct Inner {
    /// `topic/partition` → epoch, filtered down to partitions this
    /// broker leads.
    ownership: HashMap<String, u32>,
    /// `topic/partition` → broker id, full set. Drives the
    /// Metadata response's per-partition `leader` field.
    leaders: HashMap<String, String>,
    last_applied_version: i64,
}

/// Broker-side cluster coordinator. Holds the current assignment +
/// derived indices + registered change handlers + the seam traits.
pub struct Coordinator {
    self_id: BrokerId,
    /// Cluster-state directory holding `assignment.json` — the
    /// resolved `KAAS_CLUSTER_DIR` (gh #221 phase 1), NOT the data
    /// dir. Legacy layout passes `<data_dir>/__cluster` here.
    cluster_dir: PathBuf,
    lease: Arc<dyn LeaseEpochSource>,
    heartbeat: Arc<dyn HeartbeatSource>,
    current: ArcSwapOption<Assignment>,
    inner: Mutex<Inner>,
    handlers: Mutex<Vec<AssignmentChangeHandler>>,
    /// gh #62 produce-path self-fence. Off by default — dev and
    /// single-broker disk mode wire [`LocalHeartbeat`] (never any
    /// heartbeat), which would otherwise reject every write. The
    /// cluster runtime enables it once a real [`HeartbeatSource`]
    /// (the controller heartbeat stream) is wired.
    self_fence: std::sync::atomic::AtomicBool,
}

impl std::fmt::Debug for Coordinator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let inner = self.inner.lock();
        f.debug_struct("Coordinator")
            .field("self_id", &self.self_id)
            .field("cluster_dir", &self.cluster_dir)
            .field("partitions_led", &inner.ownership.len())
            .field("last_applied_version", &inner.last_applied_version)
            .finish()
    }
}

impl Coordinator {
    /// `cluster_dir` is the directory that holds `assignment.json`
    /// directly (no `__cluster` is joined here).
    pub fn new(
        self_id: impl Into<BrokerId>,
        cluster_dir: impl Into<PathBuf>,
        lease: Arc<dyn LeaseEpochSource>,
        heartbeat: Arc<dyn HeartbeatSource>,
    ) -> Arc<Self> {
        Arc::new(Self {
            self_id: self_id.into(),
            cluster_dir: cluster_dir.into(),
            lease,
            heartbeat,
            current: ArcSwapOption::empty(),
            inner: Mutex::new(Inner::default()),
            handlers: Mutex::new(Vec::new()),
            self_fence: std::sync::atomic::AtomicBool::new(false),
        })
    }

    /// Arm the gh #62 self-fence: produce acks require a controller
    /// heartbeat within [`crate::self_fence::DEFAULT_HEARTBEAT_TIMEOUT`].
    /// Call only when a real heartbeat stream is wired.
    pub fn enable_self_fence(&self) {
        self.self_fence
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }

    /// Produce-path write gate. `true` = safe to ack. Always `true`
    /// until [`Self::enable_self_fence`]; afterwards requires a
    /// fresh controller heartbeat — a broker cut off from the
    /// controller stops acking within the timeout even if its
    /// `assignment.json` view is stale (the takeover safety bound).
    pub fn heartbeat_fresh_for_writes(&self) -> bool {
        if !self.self_fence.load(std::sync::atomic::Ordering::Relaxed) {
            return true;
        }
        crate::self_fence::is_heartbeat_fresh(
            self.last_heartbeat(),
            crate::self_fence::DEFAULT_HEARTBEAT_TIMEOUT,
        )
    }

    pub fn self_id(&self) -> &str {
        &self.self_id
    }

    pub fn cluster_dir(&self) -> &Path {
        &self.cluster_dir
    }

    /// Absolute path of the assignment file this coordinator watches.
    pub fn assignment_path(&self) -> PathBuf {
        self.cluster_dir.join(Assignment::FILE_NAME)
    }

    /// Most recently applied snapshot. `None` before the first
    /// successful apply.
    pub fn snapshot(&self) -> Option<Arc<Assignment>> {
        self.current.load_full()
    }

    /// Does this broker lead `(topic, partition)` under the current
    /// assignment?
    pub fn owns(&self, topic: &str, partition: i32) -> bool {
        let k = partition_key(topic, partition);
        self.inner.lock().ownership.contains_key(&k)
    }

    /// Epoch this broker holds for `(topic, partition)`. `None` when
    /// this broker doesn't lead the partition.
    pub fn current_epoch(&self, topic: &str, partition: i32) -> Option<u32> {
        let k = partition_key(topic, partition);
        self.inner.lock().ownership.get(&k).copied()
    }

    /// Broker leading `(topic, partition)` under the current
    /// assignment. `None` when no record exists.
    pub fn leader_for(&self, topic: &str, partition: i32) -> Option<String> {
        let k = partition_key(topic, partition);
        self.inner.lock().leaders.get(&k).cloned()
    }

    /// Register an assignment-change handler. Handlers fire
    /// synchronously on the watcher task whenever a fresh
    /// assignment is applied. Long work should be deferred — the
    /// driver layer (`TakeoverDriver`, `GroupTakeoverDriver`)
    /// keeps it short by spawning storage I/O elsewhere.
    pub fn on_assignment_change(&self, handler: AssignmentChangeHandler) {
        self.handlers.lock().push(handler);
    }

    /// Spawn the watcher task. Reads the file on a 1 s tick (Phase 5
    /// ships the poll; `notify`-driven inotify is a Phase 8 perf
    /// nicety per the plan). Returns the `JoinHandle` so the caller
    /// can `.abort()` on shutdown.
    pub fn spawn_watcher(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        let this = self.clone();
        tokio::spawn(async move {
            // Best-effort initial load.
            let changed = this.apply_if_new();
            record_poll(changed);
            let mut tick = tokio::time::interval(POLL_INTERVAL);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tick.tick().await;
                let changed = this.apply_if_new();
                record_poll(changed);
            }
        })
    }

    /// Read `assignment.json`; validate the controller epoch; dedup
    /// against `last_applied_version`; on success swap `current` and
    /// fire registered handlers. Returns `true` when a fresh
    /// assignment was applied.
    pub fn apply_if_new(&self) -> bool {
        let path = self.assignment_path();
        let bytes = match std::fs::read(&path) {
            Ok(b) => b,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return false,
            Err(e) => {
                debug!(path = %path.display(), %e, "assignment.json: read failed");
                return false;
            }
        };
        let parsed: Assignment = match serde_json::from_slice(&bytes) {
            Ok(a) => a,
            Err(e) => {
                // Torn JSON mid-write is expected on first read —
                // the writer uses tmp + rename; only the new file
                // matters and we'll catch it on the next tick.
                debug!(path = %path.display(), %e, "assignment.json: parse failed (will retry)");
                return false;
            }
        };

        let lease_epoch = self.lease.current_epoch();
        if parsed.controller_epoch < lease_epoch {
            warn!(
                file_epoch = parsed.controller_epoch,
                lease_epoch, "assignment.json rejected — stale controller epoch"
            );
            kaas_observability::metrics::global()
                .stale_assignments_rejected
                .add(1, &[]);
            return false;
        }

        let prev_arc = self.current.load_full();
        {
            let inner = self.inner.lock();
            if let Some(prev) = prev_arc.as_ref() {
                if parsed.controller_epoch == prev.controller_epoch
                    && parsed.assignment_version <= inner.last_applied_version
                {
                    return false;
                }
            }
        }

        // Build derived indices first so they swap with `current` in
        // one critical section.
        let mut ownership = HashMap::new();
        let mut leaders = HashMap::new();
        for p in &parsed.partitions {
            let k = partition_key(&p.topic, p.partition);
            if p.broker == self.self_id {
                ownership.insert(k.clone(), p.epoch);
            }
            leaders.insert(k, p.broker.clone());
        }

        let new_arc = Arc::new(parsed.clone());
        {
            let mut inner = self.inner.lock();
            inner.ownership = ownership;
            inner.leaders = leaders;
            inner.last_applied_version = parsed.assignment_version;
        }
        self.current.store(Some(new_arc.clone()));

        let handlers = self.handlers.lock().clone();
        for h in handlers {
            h(prev_arc.as_deref(), &new_arc);
        }
        true
    }

    /// Wall-clock of the last received heartbeat (used by
    /// `is_heartbeat_fresh` on the produce hot path).
    pub fn last_heartbeat(&self) -> Option<tokio::time::Instant> {
        self.heartbeat.last_received()
    }
}

// --- GroupAssignmentSource impl (gh #92 hot-swap) --------------------

impl GroupAssignmentSource for Coordinator {
    fn owns_group(&self, group_id: &str) -> bool {
        let a = match self.current.load_full() {
            Some(a) => a,
            None => return false,
        };
        for g in &a.consumer_groups {
            if g.group_id == group_id {
                return g.broker == self.self_id;
            }
        }
        let (brokers, alive) = a.broker_sets();
        pick_group_coordinator(group_id, &brokers, &alive)
            .map(|b| b == self.self_id)
            .unwrap_or(false)
    }

    fn group_coordinator(&self, group_id: &str) -> Option<BrokerId> {
        let a = self.current.load_full()?;
        for g in &a.consumer_groups {
            if g.group_id == group_id {
                return Some(g.broker.clone());
            }
        }
        let (brokers, alive) = a.broker_sets();
        pick_group_coordinator(group_id, &brokers, &alive)
    }

    /// gh #167: the offset-file fence is the assignment the ownership
    /// answers above were derived from. Zero before the first apply —
    /// but `owns_group` answers `false` then too, so nothing commits.
    fn assignment_fence(&self) -> kaas_coordinator::offset_store::FenceStamp {
        match self.current.load_full() {
            Some(a) => kaas_coordinator::offset_store::FenceStamp {
                controller_epoch: a.controller_epoch,
                assignment_version: a.assignment_version,
            },
            None => kaas_coordinator::offset_store::FenceStamp::default(),
        }
    }
}

// --- TxnAssignmentSource impl (gh #91) -------------------------------

impl TxnAssignmentSource for Coordinator {
    fn owns_txn(&self, transactional_id: &str) -> bool {
        let a = match self.current.load_full() {
            Some(a) => a,
            None => return false,
        };
        let (brokers, alive) = a.broker_sets();
        pick_txn_coordinator(transactional_id, &brokers, &alive)
            .map(|b| b == self.self_id)
            .unwrap_or(false)
    }

    fn txn_coordinator(&self, transactional_id: &str) -> Option<BrokerId> {
        let a = self.current.load_full()?;
        let (brokers, alive) = a.broker_sets();
        pick_txn_coordinator(transactional_id, &brokers, &alive)
    }
}

/// Bump `kaas.assignment.polls` after every mtime tick. The
/// `change_detected` label lets dashboards
/// can distinguish "watcher is running but nothing changed" from
/// "watcher is running and just applied a fresh assignment".
fn record_poll(change_detected: bool) {
    kaas_observability::metrics::global().assignment_polls.add(
        1,
        &[kaas_observability::KeyValue::new(
            "change_detected",
            if change_detected { "true" } else { "false" },
        )],
    );
}

/// gh #208: has this broker taken over every partition the current
/// assignment gives it?
///
/// - `true` vacuously when the broker is assigned no partitions.
/// - `false` while no assignment has been applied yet (still booting).
///
/// This is the *serving* half of honest readiness — "is takeover
/// complete". It is deliberately NOT a request-path liveness check: a
/// wedged main runtime keeps its partitions open, so `open_partition_keys`
/// still lists them. Pair this with the main-runtime liveness tick
/// (`kaas_observability::health::main_alive`) for the full picture.
#[must_use]
pub fn is_serving(coordinator: &Coordinator, engine: &dyn kaas_storage::StorageEngine) -> bool {
    let Some(a) = coordinator.snapshot() else {
        return false;
    };
    let open: std::collections::HashSet<(String, i32)> =
        engine.open_partition_keys().into_iter().collect();
    a.partitions
        .iter()
        .filter(|p| p.broker == coordinator.self_id())
        .all(|p| open.contains(&(p.topic.clone(), p.partition)))
}

/// Canonical `"topic/partition"` cache key used by both ownership
/// and leader lookups.
pub fn partition_key(topic: &str, partition: i32) -> String {
    let mut s = String::with_capacity(topic.len() + 12);
    s.push_str(topic);
    s.push('/');
    s.push_str(&partition.to_string());
    s
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::assignment::{
        BrokerAssignment, BrokerHealth, ConsumerGroupAssignment, PartitionAssignment, PartitionRole,
    };

    // The tmp dir doubles as the cluster dir — `Coordinator::new`
    // takes the directory holding `assignment.json` directly.
    fn write_assignment(cluster_dir: &Path, a: &Assignment) {
        std::fs::create_dir_all(cluster_dir).unwrap();
        let path = cluster_dir.join(Assignment::FILE_NAME);
        std::fs::write(path, serde_json::to_vec(a).unwrap()).unwrap();
    }

    fn sample(version: i64, epoch: i64, lead: &str, group_broker: &str) -> Assignment {
        Assignment {
            controller_epoch: epoch,
            assignment_version: version,
            generated_at: "2025-01-01T00:00:00Z".to_owned(),
            controller: "kaas-0".to_owned(),
            brokers: vec![
                BrokerAssignment {
                    id: "kaas-0".to_owned(),
                    health: BrokerHealth::Alive,
                    last_seen: "x".to_owned(),
                },
                BrokerAssignment {
                    id: "kaas-1".to_owned(),
                    health: BrokerHealth::Alive,
                    last_seen: "x".to_owned(),
                },
            ],
            partitions: vec![PartitionAssignment {
                topic: "t1".to_owned(),
                partition: 0,
                broker: lead.to_owned(),
                epoch: u32::try_from(epoch).unwrap_or(0),
                role: PartitionRole::Leader,
            }],
            consumer_groups: vec![ConsumerGroupAssignment {
                group_id: "g1".to_owned(),
                broker: group_broker.to_owned(),
                epoch: 1,
            }],
        }
    }

    fn coordinator(self_id: &str, dir: &Path) -> Arc<Coordinator> {
        Coordinator::new(
            self_id,
            dir,
            Arc::new(LocalLeaseEpoch),
            Arc::new(LocalHeartbeat),
        )
    }

    #[test]
    fn snapshot_empty_before_first_apply() {
        let tmp = tempfile::tempdir().unwrap();
        let c = coordinator("kaas-0", tmp.path());
        assert!(c.snapshot().is_none());
        assert!(!c.owns("t1", 0));
    }

    #[test]
    fn apply_reads_assignment_and_populates_ownership() {
        let tmp = tempfile::tempdir().unwrap();
        let c = coordinator("kaas-0", tmp.path());
        write_assignment(tmp.path(), &sample(1, 1, "kaas-0", "kaas-0"));
        assert!(c.apply_if_new());
        assert!(c.owns("t1", 0));
        assert_eq!(c.current_epoch("t1", 0), Some(1));
        assert_eq!(c.leader_for("t1", 0).as_deref(), Some("kaas-0"));
        assert!(c.owns_group("g1"));
        assert_eq!(c.group_coordinator("g1").as_deref(), Some("kaas-0"));
    }

    #[test]
    fn apply_ignores_same_version_replay() {
        let tmp = tempfile::tempdir().unwrap();
        let c = coordinator("kaas-0", tmp.path());
        write_assignment(tmp.path(), &sample(1, 1, "kaas-0", "kaas-0"));
        assert!(c.apply_if_new());
        assert!(!c.apply_if_new(), "same version → no-op");
        // Bumping the version triggers another apply.
        write_assignment(tmp.path(), &sample(2, 1, "kaas-0", "kaas-0"));
        assert!(c.apply_if_new());
    }

    #[test]
    fn apply_rejects_stale_controller_epoch() {
        struct E5;
        impl LeaseEpochSource for E5 {
            fn current_epoch(&self) -> i64 {
                5
            }
        }
        let tmp = tempfile::tempdir().unwrap();
        let c = Coordinator::new("kaas-0", tmp.path(), Arc::new(E5), Arc::new(LocalHeartbeat));
        write_assignment(tmp.path(), &sample(1, 3, "kaas-0", "kaas-0"));
        assert!(!c.apply_if_new(), "epoch 3 < lease 5 → rejected");
        write_assignment(tmp.path(), &sample(1, 5, "kaas-0", "kaas-0"));
        assert!(c.apply_if_new(), "epoch 5 == lease 5 → accepted");
    }

    #[test]
    fn assignment_change_handler_fires_with_prev_and_next() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let tmp = tempfile::tempdir().unwrap();
        let c = coordinator("kaas-0", tmp.path());
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_c = calls.clone();
        c.on_assignment_change(Arc::new(move |prev, next| {
            calls_c.fetch_add(1, Ordering::Relaxed);
            if calls_c.load(Ordering::Relaxed) == 1 {
                assert!(prev.is_none(), "first apply: prev is None");
                assert_eq!(next.assignment_version, 1);
            } else {
                assert!(prev.is_some(), "second apply: prev present");
                assert_eq!(next.assignment_version, 2);
            }
        }));
        write_assignment(tmp.path(), &sample(1, 1, "kaas-0", "kaas-0"));
        c.apply_if_new();
        write_assignment(tmp.path(), &sample(2, 1, "kaas-0", "kaas-0"));
        c.apply_if_new();
        assert_eq!(calls.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn owns_group_two_tier_explicit_override_wins() {
        let tmp = tempfile::tempdir().unwrap();
        let c0 = coordinator("kaas-0", tmp.path());
        let c1 = coordinator("kaas-1", tmp.path());
        // Explicit override pins g1 to kaas-1 even if hash routes
        // it to kaas-0.
        write_assignment(tmp.path(), &sample(1, 1, "kaas-0", "kaas-1"));
        c0.apply_if_new();
        c1.apply_if_new();
        assert!(c1.owns_group("g1"));
        assert!(!c0.owns_group("g1"));
    }

    #[test]
    fn group_coordinator_falls_back_to_hash_when_no_explicit_entry() {
        let tmp = tempfile::tempdir().unwrap();
        let c = coordinator("kaas-0", tmp.path());
        let mut a = sample(1, 1, "kaas-0", "kaas-0");
        // Drop the explicit consumer-groups entry — hash routes the
        // group through the broker set.
        a.consumer_groups.clear();
        write_assignment(tmp.path(), &a);
        c.apply_if_new();
        // Hash returns *some* broker from {kaas-0, kaas-1};
        // either way the resolve succeeds.
        assert!(c.group_coordinator("g-fresh").is_some());
    }

    #[test]
    fn txn_assignment_source_uses_hash() {
        let tmp = tempfile::tempdir().unwrap();
        let c = coordinator("kaas-0", tmp.path());
        write_assignment(tmp.path(), &sample(1, 1, "kaas-0", "kaas-0"));
        c.apply_if_new();
        assert!(c.txn_coordinator("tx-1").is_some());
    }

    #[test]
    fn missing_assignment_file_is_not_fatal() {
        let tmp = tempfile::tempdir().unwrap();
        let c = coordinator("kaas-0", tmp.path());
        assert!(!c.apply_if_new());
        assert!(c.snapshot().is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn spawn_watcher_picks_up_new_file() {
        let tmp = tempfile::tempdir().unwrap();
        let c = coordinator("kaas-0", tmp.path());
        let handle = c.spawn_watcher();
        // Yield once for the initial apply (file missing → no-op).
        tokio::task::yield_now().await;
        write_assignment(tmp.path(), &sample(1, 1, "kaas-0", "kaas-0"));
        // Advance past the 1 s poll interval; the watcher should
        // pick up the file.
        tokio::time::advance(POLL_INTERVAL + Duration::from_millis(100)).await;
        tokio::task::yield_now().await;
        assert!(c.snapshot().is_some());
        handle.abort();
    }
}
