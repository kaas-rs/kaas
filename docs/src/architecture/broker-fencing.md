# Broker fencing

A Kafka cluster has three answers to "what happened to that broker?", and only
two of them are visible in a Metadata response. The broker is **serving**, it is
**gone**, or it is **registered but not serving** — Kafka calls the third state
*fenced*. Metadata omits fenced brokers entirely, so a client that only asks
Metadata sees a three-broker cluster become a two-broker cluster, with nothing
to say whether that was a scale-down or a crash.

kaas answers all three. This page is how.

## What "registered" means here

In Apache Kafka (KRaft), a broker registers with the controller quorum, gets a
broker epoch, and heartbeats. Miss `broker.session.timeout.ms` and the
controller fences it: still registered, no longer leader-eligible, dropped from
Metadata. Deregistration is a separate, explicit act — an operator running
`kafka-cluster.sh unregister` for a broker that is never coming back.

kaas has no metadata quorum to register against; controller election runs on a
Kubernetes Lease (see [Non-goals](../compat/non-goals.md)). So it registers
against the thing that already knows which brokers are supposed to exist: the
headless Service's **EndpointSlices**. The broker watches them anyway, to learn
where its peers are.

That gives the three states without inventing a registry:

| state | EndpointSlice | meaning |
|---|---|---|
| serving | listed, `Ready` | assignable, advertised |
| fenced | listed, not `Ready` | exists, not serving |
| gone | not listed | deregistered |

Kubernetes supplies the hard part for free. A scale-down removes the endpoint,
which *is* the deregistration — no `unregister` verb, no operator ceremony, no
tombstone to garbage-collect.

## Where each state is produced

`BrokerRegistry` (`crates/kaas-k8s/src/endpoints.rs`) keeps every endpoint the
slice lists, carrying its readiness rather than filtering on it. Two rules make
that safe:

- **An ordinal absent from its slice is deregistered.** A real EndpointSlice
  update always carries the slice's whole membership, so "absent" is
  meaningful. Without this, keeping not-ready entries would leak a scaled-away
  broker into every response for the life of the process. The removal is scoped
  to the slice that *owns* the ordinal, because a Service's endpoints may be
  sharded across several slices and a naive sweep would have each slice evict
  the others' brokers.
- **Self is pinned.** This broker is never inserted, downgraded, or removed by
  slice data. A readiness blip on its own pod must not make it forget it
  exists — that failure was observed live, and it is self-sustaining: self
  eviction → the controller balances over an empty set → every partition
  unassigned → the resulting takeover storm fails the next probe too.

The controller turns that into cluster-wide state. `assignment.json`'s broker
list has always had the shape for it —

```rust
pub enum BrokerHealth { Alive, Draining, Dead }
```

— and now has a producer: every **registered** broker gets a row, marked
`Alive` if the heartbeat says so, `Draining` if it announced a shutdown, and
`Dead` otherwise. A fenced broker keeps its `last_seen` from when it was last
alive, rather than being refreshed to "just now" on every recompute.

## The coordinator-divisor bug this fixed

Reporting was the motivation; correctness was the surprise.

`group_hash` picks a group's coordinator with `hash(groupID) % num_brokers`,
and its documentation is emphatic that the divisor must be the **full** broker
set, "including draining / dead" — holding it constant is what keeps group
coordinatorship stable across restarts. But the list it divides by came from
`assignment.json`, and that list was the *alive* set: a dead broker was dropped
rather than marked. So losing one broker of three silently changed the divisor
from 3 to 2 and rehashed roughly two-thirds of all group and transaction
coordinators — precisely when the cluster was already degraded and least able
to absorb the churn.

With the tri-state, only the groups that actually lived on the lost broker
move. `a_dead_broker_moves_only_its_own_groups` in
`crates/kaas-broker/src/assignment.rs` pins that: of 200 groups across three
brokers, a broker loss moves the ~1/3 that hashed to it and leaves the rest
where they were.

## Draining: fencing from the other direction

A broker that is shutting down is not unhealthy — it serves normally right up
until its listeners close. But it is leaving, and the controller used to find
that out the slow way, by timing out a heartbeat from a process that had
already exited.

The SIGTERM path now calls `mark_draining()` **before** anything is torn down.
The next heartbeat (~1 s) carries `draining = true`; the controller drops the
broker from the alive set immediately — moving its partitions and group
coordinatorships while it is still healthy enough to hand them over — and marks
its row `Draining` rather than `Dead`.

Two properties are load-bearing. The broker stays in the *registered* list, so
the divisor above doesn't move. And the self-pin in the alive-set policy
outranks draining: a draining controller keeps itself in the set, because an
empty alive set would unassign the entire cluster.

This is the proactive half of graceful shutdown — controlled shutdown and
fencing are the same feature approached from two sides.

## What clients see

Metadata omits fenced brokers, exactly as Apache does: advertising one would
send clients to a broker that cannot answer.

[DescribeCluster](../compat/api/cluster-misc.md#describecluster) v2 reports
them, with `IsFenced` per row, when the request sets `IncludeFencedBrokers`.
That version is [KIP-1073](https://cwiki.apache.org/confluence/x/uYuMEg) —
Kafka 4.0 surface, and a deliberate exception to kaas's Apache 3.7 parity
target, taken because there is now a real fenced state and no other way to
report it.

The broker answering a request never reports *itself* as fenced. It is
serving — it just answered.

## Notes for contributors

- `BrokerHealth::Dead` vs *absent from the list* is the fenced/gone
  distinction. Don't "clean up" dead rows: they are the divisor.
- Fenced is derived from EndpointSlice readiness, so a **booting** broker reads
  as fenced until takeover completes and `/readyz` flips. That is deliberate
  and matches Apache, which fences a registered broker until it has caught up.
  It also means a rolling restart shows a fenced broker at each step — which is
  the honest report, not a defect.
- Readiness and the alive set are still separate signals, and conflating them
  is the gh #208 trap: the alive set is driven by the heartbeat's `healthy`
  bit, not by readiness, so a booting broker stays *assignable* even while it
  reads as fenced for reporting.
- Relevant source: `crates/kaas-k8s/src/endpoints.rs` (registry),
  `crates/kaas-controller/src/assignment_writer.rs` (`build_broker_entries`),
  `bins/kaas/src/cluster.rs` (`decide_alive`, the registered/draining sources),
  `crates/kaas-broker/src/handlers/describe_cluster.rs` (the wire surface).
