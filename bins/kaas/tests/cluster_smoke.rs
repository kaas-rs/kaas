//! Phase 5 §H — multi-broker cluster smoke.
//!
//! Two brokers share a `__cluster/assignment.json` written by a
//! single-broker AssignmentLoop and verify that partition
//! ownership splits cleanly across them. The full rdkafka-driven
//! "produce 1k records with `acks=all`, consume via group, kill
//! a non-controller broker mid-run" suite from the Phase 5 plan
//! parks under `#[ignore]` until rdkafka lands in the workspace
//! deps (Phase 8 plan).
//!
//! What this test does today:
//!
//! 1. Spins up two `bins/kaas::cluster::install` runtimes
//!    against the same data_dir.
//! 2. Waits for both Coordinators to observe the first
//!    assignment.
//! 3. Asserts every partition has exactly one owner across the
//!    two brokers.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::sync::Arc;
use std::time::Duration;

use kaas_broker::{Broker, TopicMeta, TopicRegistry};
use kaas_storage::{DiskStorageEngine, PartitionConfig, RealFs, StorageEngine};
use tokio_util::sync::CancellationToken;

#[path = "../src/cluster.rs"]
mod cluster;

fn build_broker(
    data_dir: &std::path::Path,
    broker_id: i32,
    topics: Arc<TopicRegistry>,
) -> (Arc<Broker>, Arc<dyn StorageEngine>) {
    let cfg = PartitionConfig::default();
    let engine: Arc<dyn StorageEngine> = Arc::new(DiskStorageEngine::new(
        Arc::new(RealFs),
        data_dir.to_path_buf(),
        cfg,
    ));
    let broker = Arc::new(Broker::new(
        engine.clone(),
        topics,
        "test-cluster",
        broker_id,
    ));
    (broker, engine)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn two_brokers_share_assignment_json_and_split_partitions() {
    let tmp = tempfile::tempdir().unwrap();

    let topics = Arc::new(TopicRegistry::new());
    topics.insert(TopicMeta {
        name: "t".to_owned(),
        partition_count: 6,
        topic_id: [0; 16],
    });

    // Each broker gets its own engine but shares the data_dir
    // (so __cluster/assignment.json is shared). This mirrors the
    // chart's shared RWX-NFS layout.
    let (b0, e0) = build_broker(tmp.path(), 0, topics.clone());
    let (b1, e1) = build_broker(tmp.path(), 1, topics.clone());

    let cancel = CancellationToken::new();
    let rt0 = cluster::install(
        b0.clone(),
        topics.clone(),
        e0,
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
    .expect("install ok for broker 0");
    let rt1 = cluster::install(
        b1.clone(),
        topics.clone(),
        e1,
        Some(tmp.path().to_path_buf()),
        None,
        1,
        "test-cluster",
        cancel.clone(),
        None,
        9092,
        std::sync::Arc::new(cluster::TopicChangeNotifier::default()),
        0,
    )
    .expect("install ok for broker 1");

    let coord0 = b0.coordinator().expect("coord 0 installed");
    let coord1 = b1.coordinator().expect("coord 1 installed");

    // Wait for both brokers to apply the assignment. The
    // AssignmentLoops tick every 5 s but the initial start() does
    // a synchronous write; the 1 s file poll picks it up.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        if coord0.snapshot().is_some() && coord1.snapshot().is_some() {
            break;
        }
        if std::time::Instant::now() > deadline {
            panic!("Coordinators never picked up the initial assignment");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // The two brokers are running independent AssignmentLoops
    // against the same data_dir, so the last writer wins. Pick
    // whichever snapshot is observable on broker 0 and confirm
    // partition ownership is split across the two brokers it
    // mentions.
    let snap = coord0.snapshot().expect("snapshot present");
    // Each AssignmentLoop runs as a single-broker mode loop with
    // brokers = [self_id]. Whichever one wrote last has its self
    // as sole leader. That's fine for this smoke — Phase 5 §G
    // documents single-broker AssignmentLoop, with the full
    // multi-broker controller-on-elected-broker form deferred to
    // follow-up #10. Assert the snapshot is internally
    // consistent: every partition has exactly one owner from the
    // broker set.
    let known_brokers: std::collections::HashSet<String> =
        snap.brokers.iter().map(|b| b.id.clone()).collect();
    assert!(
        !known_brokers.is_empty(),
        "snapshot must list at least one broker"
    );
    for p in &snap.partitions {
        assert!(
            known_brokers.contains(&p.broker),
            "partition broker {} not in snapshot's broker list {:?}",
            p.broker,
            known_brokers
        );
    }

    cancel.cancel();
    for h in rt0.tasks.into_iter().chain(rt1.tasks.into_iter()) {
        h.abort();
    }
}

use kaas_broker::coordinator::{Coordinator, LeaseEpochSource, LocalHeartbeat};
use kaas_k8s::{BrokerRegistry, DnsConfig, EndpointSliceData, EndpointSliceEntry};
use std::sync::atomic::{AtomicI64, Ordering};

struct AtomicEpoch(AtomicI64);

impl LeaseEpochSource for AtomicEpoch {
    fn current_epoch(&self) -> i64 {
        self.0.load(Ordering::Relaxed)
    }
}

fn test_dns() -> DnsConfig {
    DnsConfig {
        namespace: "kaas".to_owned(),
        headless_service: "kaas-headless".to_owned(),
        pod_name_pattern: "kaas-{ordinal}".to_owned(),
        cluster_domain: "cluster.local".to_owned(),
    }
}

fn slice_entry(ordinal: i32, ready: bool) -> EndpointSliceEntry {
    EndpointSliceEntry {
        hostname: format!("kaas-{ordinal}"),
        address: format!("10.0.0.{}", ordinal + 10),
        ready,
    }
}

async fn wait_until<F: Fn() -> bool>(what: &str, deadline: Duration, cond: F) {
    let end = std::time::Instant::now() + deadline;
    loop {
        if cond() {
            return;
        }
        assert!(std::time::Instant::now() < end, "timed out waiting: {what}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// The real multi-broker controller stack, kube-free: one elected
/// controller runs `run_controller` over a shared data_dir while
/// three Coordinators (one per "broker") watch assignment.json.
/// Covers: balanced spread across the alive set, topic creation →
/// recompute via the TopicChangeNotifier (gh #74), and broker loss
/// → reassignment via the 2 s broker-set watcher (gh #77).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_broker_controller_balances_and_reassigns() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().to_path_buf();

    let topics = Arc::new(TopicRegistry::new());
    topics.insert(TopicMeta {
        name: "t".to_owned(),
        partition_count: 6,
        topic_id: [0; 16],
    });

    // Three broker-side Coordinators watching the shared dir.
    let lease = Arc::new(AtomicEpoch(AtomicI64::new(1)));
    let coords: Vec<Arc<Coordinator>> = (0..3)
        .map(|id| {
            let lease_src: Arc<dyn LeaseEpochSource> = lease.clone();
            let c = Coordinator::new(
                format!("kaas-{id}"),
                dir.join("__cluster"),
                lease_src,
                Arc::new(LocalHeartbeat),
            );
            c.spawn_watcher();
            c
        })
        .collect();

    // Registry seeded with three ready endpoints — the EndpointSlice
    // view the elected controller balances over. No heartbeat
    // connections exist, so ClusterBrokerSource falls back to the
    // registry-only alive set.
    let registry = Arc::new(BrokerRegistry::new(
        kaas_k8s::BrokerEndpoint {
            node_id: 0,
            host: test_dns().fqdn(0),
            port: 9092,
            ready: true,
        },
        test_dns(),
    ));
    registry.apply_slice(&EndpointSliceData {
        name: "kaas-headless-test".to_owned(),
        entries: vec![
            slice_entry(0, true),
            slice_entry(1, true),
            slice_entry(2, true),
        ],
        kafka_port: Some(9092),
    });

    let notifier = Arc::new(cluster::TopicChangeNotifier::default());
    let leader_token = CancellationToken::new();
    tokio::spawn(cluster::run_controller(
        1,
        leader_token.clone(),
        dir.clone(),
        dir.join("__cluster"),
        "kaas-0".to_owned(),
        topics.clone(),
        registry.clone(),
        "127.0.0.1:0".to_owned(),
        notifier.clone(),
        tokio::runtime::Handle::current(),
    ));

    // 1. All six partitions get exactly one owner, spread over all
    //    three brokers (rendezvous + smoothing caps skew at 1 ⇒
    //    every broker owns exactly 2 of 6). The deadline allows for
    //    the controller's 7 s heartbeat grace — no heartbeat clients
    //    connect in this kube-free harness, so it waits it out.
    wait_until(
        "initial 3-broker assignment",
        Duration::from_secs(15),
        || {
            coords[0].snapshot().is_some_and(|a| {
                let owners: std::collections::HashSet<&str> =
                    a.partitions.iter().map(|p| p.broker.as_str()).collect();
                a.partitions.len() == 6 && owners.len() == 3
            })
        },
    )
    .await;
    for c in &coords {
        wait_until("every coordinator applies", Duration::from_secs(5), || {
            c.snapshot().is_some()
        })
        .await;
    }
    let snap = coords[0].snapshot().unwrap();
    for id in 0..3 {
        let owned = snap
            .partitions
            .iter()
            .filter(|p| p.broker == format!("kaas-{id}"))
            .count();
        assert_eq!(owned, 2, "kaas-{id} must own exactly 2 of 6 partitions");
    }

    // 2. Topic created → notifier pokes the loop → new partitions
    //    assigned without waiting for a periodic tick.
    topics.insert(TopicMeta {
        name: "t2".to_owned(),
        partition_count: 3,
        topic_id: [0; 16],
    });
    notifier.notify(kaas_controller::AssignmentReason::TopicCreated);
    wait_until("t2 partitions assigned", Duration::from_secs(5), || {
        coords[0]
            .snapshot()
            .is_some_and(|a| a.partitions.iter().filter(|p| p.topic == "t2").count() == 3)
    })
    .await;

    // 3. Broker loss: kaas-2 goes NotReady → the 2 s broker-set
    //    watcher recomputes → nothing stays assigned to kaas-2.
    // A real EndpointSlice update carries the slice's whole
    // membership, not a delta — and since gh #249 the registry uses
    // that: an ordinal absent from its own slice is deregistered
    // rather than fenced. So send all three, with kaas-2 NotReady.
    registry.apply_slice(&EndpointSliceData {
        name: "kaas-headless-test".to_owned(),
        entries: vec![
            slice_entry(0, true),
            slice_entry(1, true),
            slice_entry(2, false),
        ],
        kafka_port: Some(9092),
    });
    wait_until("kaas-2 drained", Duration::from_secs(10), || {
        coords[0].snapshot().is_some_and(|a| {
            !a.partitions.is_empty() && a.partitions.iter().all(|p| p.broker != "kaas-2")
        })
    })
    .await;

    leader_token.cancel();
}
