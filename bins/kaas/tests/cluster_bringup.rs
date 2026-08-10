//! Phase 5 §G smoke — the single-broker-disk wiring:
//! AssignmentLoop writes `assignment.json`, the Coordinator's
//! watcher picks it up, ownership stamps onto the broker, and the
//! consumer-group Manager surfaces work through the dispatcher.
//!
//! This test uses the same `cluster::install` helper `bins/kaas`
//! does in production, so it covers the wire-up bug class the
//! Phase 5 plan was designed to surface (Coordinator hot-swap,
//! takeover-driver dispatch, AssignmentLoop epoch monotonicity).

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::sync::Arc;
use std::time::Duration;

use kaas_broker::{Broker, TopicMeta, TopicRegistry};
use kaas_storage::{DiskStorageEngine, PartitionConfig, RealFs, StorageEngine};
use tokio_util::sync::CancellationToken;

// The cluster module is private to the binary. Re-include it here
// via the `path` attribute so the integration test exercises the
// real helper without duplicating the wiring.
#[path = "../src/cluster.rs"]
mod cluster;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn single_broker_disk_mode_wires_coordinator_end_to_end() {
    let tmp = tempfile::tempdir().unwrap();

    let topics = Arc::new(TopicRegistry::new());
    topics.insert(TopicMeta {
        name: "t1".to_owned(),
        partition_count: 3,
        topic_id: [0; 16],
    });

    let cfg = PartitionConfig::default();
    let engine: Arc<dyn StorageEngine> = Arc::new(DiskStorageEngine::new(
        Arc::new(RealFs),
        tmp.path().to_path_buf(),
        cfg,
    ));
    let broker = Arc::new(Broker::new(
        engine.clone(),
        topics.clone(),
        "test-cluster",
        0,
    ));

    let cancel = CancellationToken::new();
    let runtime = cluster::install(
        broker.clone(),
        topics.clone(),
        engine.clone(),
        Some(tmp.path().to_path_buf()),
        None,
        0,
        "test-cluster",
        cancel.clone(),
        None,
        9092,
        std::sync::Arc::new(cluster::TopicChangeNotifier::default()),
        0,
    )
    .expect("install ok");

    // The Manager is installed regardless of disk mode.
    assert!(
        broker.coord_manager().is_some(),
        "Manager must be installed on the Broker"
    );
    // In disk mode the Coordinator is also installed.
    assert!(
        broker.coordinator().is_some(),
        "Coordinator must be installed when a data_dir is set"
    );

    // Wait for the AssignmentLoop's initial write + the
    // Coordinator's 1 s poll to pick it up. The loop runs an
    // initial recompute as part of start(); the watcher polls
    // every 1 s. Give it 2 s of slack.
    let coord = broker.coordinator().unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        if coord.owns("t1", 0) && coord.owns("t1", 1) && coord.owns("t1", 2) {
            break;
        }
        if std::time::Instant::now() > deadline {
            let snap = coord.snapshot();
            panic!(
                "Coordinator never claimed ownership of t1 partitions within 5s; \
                 snapshot={:?}",
                snap.as_deref()
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let snap = coord.snapshot().expect("snapshot after first apply");
    assert_eq!(snap.controller, "kaas-0");
    assert_eq!(snap.brokers.len(), 1);
    assert_eq!(snap.partitions.len(), 3);

    // Shut down: cancel + abort the background tasks.
    cancel.cancel();
    for h in runtime.tasks {
        h.abort();
    }
}

#[tokio::test]
async fn in_memory_mode_installs_manager_but_no_coordinator() {
    // No data_dir → MemoryStorage path. Manager comes up; the
    // Coordinator stays unset and Produce/Fetch fall back to
    // LocalLeaseManager.
    use kaas_storage::MemoryStorage;
    let topics = Arc::new(TopicRegistry::new());
    let engine: Arc<dyn StorageEngine> = Arc::new(MemoryStorage::new());
    let broker = Arc::new(Broker::new(engine.clone(), topics.clone(), "dev", 0));
    let cancel = CancellationToken::new();
    let runtime = cluster::install(
        broker.clone(),
        topics,
        engine,
        None,
        None,
        0,
        "dev",
        cancel.clone(),
        None,
        9092,
        std::sync::Arc::new(cluster::TopicChangeNotifier::default()),
        0,
    )
    .expect("install ok");

    assert!(broker.coord_manager().is_some());
    assert!(
        broker.coordinator().is_none(),
        "Coordinator must stay unwired in MemoryStorage mode"
    );
    // Phase 6 unconditionally spawns the FenceWatcher, the txn-
    // timeout reaper, and the MarkerWatcher (gh #175) so the
    // transactional surface works in dev mode against
    // MemoryStorage. The assignment loop + assignment.json watcher
    // stay skipped (no data dir).
    assert_eq!(
        runtime.tasks.len(),
        3,
        "dev mode should spawn the Phase 6 FenceWatcher + reaper + MarkerWatcher"
    );
    assert!(broker.txn_state().is_some());
    cancel.cancel();
}
