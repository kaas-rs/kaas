# kaas-coordinator

The consumer-group coordinator and offset store, plus the transaction coordinator's state store, marker queue, and fence log.

Two coordinators share this crate because they share a shape: in-memory
protocol state + durable files on the shared volume + an *ownership seam*
that decides which broker answers for which key.

**Consumer-group side**: `group.rs` (the join/sync/heartbeat/leave state
machine, generation tracking, static-membership handling), `manager.rs`
(group lookup/creation, ownership-filtered list/describe, offset deletion —
including `Manager::purge_topic_offsets`, which drops a deleted topic's
committed offsets from every owned group, gh #240),
`offset_store.rs` (per-group JSON files under
`<KAAS_CLUSTER_DIR>/__consumer_offsets/<group>.json` — moved under the
cluster dir by gh #223 — plus the **pending** layer that stages
transactional offsets keyed by `(groupID, PID)` until EndTxn commits them).

**Transaction side**: `txn_state.rs` (per-`transactional.id` entries,
slot-sharded across `txn_state/slot-N.json`; all transitions are atomic
slot-file rewrites), `marker_queue.rs` (cross-broker COMMIT/ABORT marker
dispatch as files under `marker_queue/to-<broker>/`), `fence_log.rs`
(cross-broker producer-epoch fence broadcast), plus the `atomic_write.rs`
and `errors.rs` helpers. The architecture chapters on
[transactions](../architecture/transactions.md) and
[consumer groups](../architecture/consumer-groups.md) explain why these are
files and not RPCs.

**The invariant callers must hold — the assignment-source indirection**:
group ownership comes from a `GroupAssignmentSource`, txn ownership from a
txn-assignment source. In production both are backed by the broker
`Coordinator` (hash-fallthrough over `assignment.json`); in single-broker
tests they're local always-true stubs. Code in this crate must route every
"do I own this?" question through the seam — hard-coding locality
reintroduces the gh #92 chicken-and-egg that took two releases to unwind.

**Start reading at** `manager.rs`, then `group.rs` for the rebalance state
machine, then `txn_state.rs`.
