# kaas-controller

Controller-side logic: Lease election, the partition/group balancer, the assignment writer, and the heartbeat gRPC server.

Everything that runs *only* on the broker currently holding the
`kaas-controller` Lease ([architecture](../architecture/controller.md)).

**Module map**: `election.rs` (the election seam) + `kube_election.rs` (the
Kubernetes Lease implementation; `LocalElection` is the dev-mode
always-elected stub), `balancer.rs` (partition + consumer-group placement
with deterministic smoothing, so recomputes move as little as possible),
`assignment_writer.rs` (atomic `assignment.json` writes behind the
`TopicSource` / `BrokerSource` / `GroupSource` / `CrMirror` trait seams),
`heartbeat_server.rs` (the bidi gRPC server side of
`proto/heartbeat.proto`), `k8s_mirror.rs` (the `CrMirror` trait seam for
the `KafkaClusterAssignments` debug mirror — only a `NoopMirror` ships
today; the kube-backed status writer is un-started follow-up).

**The trait seams are the point**: the balancer and writer are pure over
their sources, which is what makes
`tests/controller_failover.rs` and `tests/stale_controller_race.rs`
possible without a cluster — the stale-epoch fence (a deposed controller's
write rejected by its stale `leaseTransitions` epoch) is pinned by test,
not by hope.

**Invariant callers must hold**: the assignment file is the *only* output
channel. Nothing in this crate may instruct a broker directly — brokers
follow `assignment.json`, and anything the controller wants to happen must
be expressible as an assignment change.

**Start reading at** `balancer.rs`, then `assignment_writer.rs`.
