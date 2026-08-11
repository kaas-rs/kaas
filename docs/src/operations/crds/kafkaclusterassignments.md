# KafkaClusterAssignments

`KafkaClusterAssignments` is the odd one out: it is **not
configuration**. It is reserved as a read-only debug mirror of the
cluster's partition-assignment state — and, honestly up front: **the
mirror is not wired up yet**. The CR exists in every cluster (the
operator creates one per `KafkaCluster`, empty, with an
OwnerReference), the status schema below is defined, and the broker
RBAC already permits status writes — but nothing writes it today, so
`kubectl get kafkaclusterassignments` shows blank columns.

The state it is meant to mirror is real and inspectable: the
controller-written `assignment.json` on the shared volume, the single
source of truth for partition leadership (see [Controller, leases &
assignment.json](../../architecture/controller.md)). In Apache Kafka
you would answer "who leads partition 3 of `orders`?" with
`kafka-topics.sh --describe` or by querying the metadata quorum. Both
of those work against kaas — Metadata is served normally, and
`kafka-topics.sh --describe` shows leadership — so the CR is a
convenience surface, not the only window.

## Reading the assignment today

```bash
# The authoritative file, from any broker pod:
kubectl exec -n kafka kaas-0 -- cat /data/__cluster/assignment.json | jq .

# Or the client view:
kafka-topics.sh --bootstrap-server <bootstrap> --describe --topic orders
```

The file carries the controller epoch (the fencing token brokers use
to reject a deposed controller's late writes), a version counter,
every **registered** broker with its health tri-state (`alive`,
`draining`, `dead` — dead rows are deliberately retained, see [Broker
fencing](../../architecture/broker-fencing.md)), the partition →
leader map with per-partition epochs, and explicit consumer-group
coordinator placements.

## The CR's intended shape

Properties that follow from "mirror, not source", and already hold:

- **`spec` is empty and inert.** Editing this CR does nothing; brokers
  never read it.
- **One per cluster**, sharing the `KafkaCluster`'s name and
  namespace, garbage-collected with it via OwnerReference.

The status schema, for when the writer lands (printer columns
`Controller` / `Epoch` / `Version` / `Truncated` map to the first
four): `controller` (lease holder), `controllerEpoch` (lease
transition count at write time), `assignmentVersion` (monotonic within
a controller's tenure), `generatedAt`, `truncated` (set when the
partition list is clipped to fit the 1 MB Kubernetes object limit —
the file is always complete), `brokers[]` (id / tri-state health /
`last_seen`), `partitions[]` (topic / partition / broker / epoch), and
`consumerGroups[]` (explicit coordinator placements; groups absent
from the list are coordinated by the deterministic hash fallback).

## Implementation notes (for contributors)

- Type: `crates/kaas-operator-api/src/kafkaclusterassignments.rs`;
  generated schema `deploy/crds/kaas.rs_kafkaclusterassignments.yaml`.
- The mirror seam exists but is unfilled: the controller crate ships
  the `CrMirror` trait with only a `NoopMirror`
  (`crates/kaas-controller/src/k8s_mirror.rs`); the kube-backed status
  writer attaching via `AssignmentLoop::with_mirror` is the open
  follow-up.
- The operator's create-only mint (with OwnerReference) lives in
  `crates/kaas-operator-controllers/src/kafkacluster_controller.rs`;
  broker RBAC for `kafkaclusterassignments/status` is already in
  `deploy/helm/kaas/templates/broker-rbac.yaml`.
