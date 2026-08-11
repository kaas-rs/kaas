# CRD reference

Everything you would manage in Apache Kafka through the Admin API or
ZooKeeper-era shell tools — topics, users, ACLs, quotas, cluster
plumbing — is declared in kaas as Kubernetes **custom resources** under
the `kaas.rs/v1alpha1` API group. If you have run Kafka under Strimzi
the shape is deliberately familiar; where a kaas CRD diverges from its
Strimzi counterpart, the per-CRD page says so and why.

This chapter is the field-level reference. The architectural story —
what reconciliation produces, why there are no finalizers, how brokers
consume the CRs — lives in [Kubernetes
integration](../architecture/kubernetes.md).

| CRD | Kind | What it drives | Reference |
|---|---|---|---|
| `kafkaclusters.kaas.rs` | `KafkaCluster` | External-listener plumbing: certificates, per-broker Services, TLSRoutes | [KafkaCluster](./crds/kafkacluster.md) |
| `kafkatopics.kaas.rs` | `KafkaTopic` | Topic existence, partition count, per-topic config, volume placement | [KafkaTopic](./crds/kafkatopic.md) |
| `kafkausers.kaas.rs` | `KafkaUser` | Credentials, ACLs, quotas — one CR per principal | [KafkaUser](./crds/kafkauser.md) |

All three are namespaced. The examples in this chapter use the `kafka`
namespace from [Getting Started](../getting-started.md).

## Installing and upgrading the CRDs

The CRD YAML ships bundled with the Helm chart (`deploy/helm/kaas/crds/`)
and is also published standalone under `deploy/crds/`. Helm installs
bundled CRDs on first `helm install` but **deliberately never upgrades
them** — after upgrading the chart, apply the new CRD YAML yourself:

```bash
kubectl apply -f deploy/crds/
```

See [Helm chart & listener configuration](./helm.md) for the full
upgrade procedure and the pre-v1 compatibility rules that go with it.

## Conventions shared by every kaas CRD

- **Status conditions.** Each reconciled CRD reports a `Ready`
  condition in `status.conditions`, surfaced as a printer column, so
  `kubectl get kafkatopics` (or `kafkausers`, `kafkaclusters`) shows
  reconcile health at a glance. A `Ready=False` condition carries a
  reason and message naming what was rejected.
- **No apiserver defaulting for enum-like strings.** Fields such as an
  ACL's `patternType` or an issuer's `kind` are left empty in the
  stored object when you omit them; the operator applies the default
  at reconcile time. Your stored CR stays byte-for-byte what you
  wrote, which keeps GitOps diffs clean.
- **No finalizers.** Deleting a CR never blocks on the operator being
  alive. Owned Kubernetes resources are garbage-collected via
  OwnerReferences; on-disk state is reclaimed by a leader-elected
  sweep. The [Kubernetes integration
  page](../architecture/kubernetes.md) explains the ArgoCD deadlock
  that motivated this.
- **Deletion is destructive and unguarded.** There is no
  spec-level "protection" flag yet: deleting a `KafkaTopic` deletes
  its data (and its committed consumer offsets, matching Apache),
  deleting a `KafkaUser` revokes its credential and ACLs.

## Implementation notes (for contributors)

- CRD types are kube-derive structs in
  `crates/kaas-operator-api/src/`, one module per kind. `cargo xtask
  gen-crds` regenerates `deploy/crds/` and the chart copy; the `rust`
  CI job fails on drift, so commit both when you touch the types.
- Reconcilers live in `crates/kaas-operator-controllers/`.
