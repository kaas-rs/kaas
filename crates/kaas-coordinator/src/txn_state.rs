//! Transaction-coordinator persistent state store.
//!
//! Tracks
//! `(producer_id, epoch, state, partitions, groups, ongoing_since_ms,
//! transaction_timeout_ms)` per `transactional.id`, sharded across
//! 50 JSON files under `<data_dir>/__cluster/txn_state/slot-<n>.json`.
//!
//! Mirrors Apache Kafka's `__transaction_state` internal topic:
//! partition = slot, log replay = JSON file read. Kaas skips the
//! log-replay step because the file *is* the materialised map the
//! Apache coordinator builds from compacted log records.
//!
//! **Read-fresh-on-every-call** semantics: each public method
//! re-reads the slot file from disk before mutating, then writes
//! back via atomic `tmp + fsync + rename`. NFS close-to-open
//! consistency means a fresh `File::open` sees the latest committed
//! state from any other broker that recently wrote — so on
//! coordinator failover the new owner continues from the same
//! (PID, epoch) state without log replay.
//!
//! **Known gaps**:
//! - pre-#108 single-file → slot-layout migration is not implemented —
//!   only the slot layout ships; ancient deployments upgrade through
//!   a stop-the-world cutover.
//! - `migrateLayout` (slot count re-shard) — `NUM_SLOTS` is pinned
//!   to `DEFAULT_NUM_SLOTS = 50` for the whole cluster.

use std::collections::HashMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::atomic_write::atomic_write_json;

/// Matches Apache Kafka's `transaction.state.log.num.partitions=50`
/// default. Pinning to a fixed cluster-wide constant decouples the
/// storage layout from broker scale operations.
///
/// **Do not change this value on a deployed cluster.** The on-disk
/// slot is computed as `fnv1a_32(txn_id) % NUM_SLOTS`; a different
/// constant moves every existing entry to a different slot file and
/// the next `get_or_allocate` for an existing `txn_id` reads an
/// empty slot — silently breaking the gh #22 rejoin contract. Apache
/// enforces this by reading `transaction.state.log.num.partitions`
/// at first cluster start and ignoring later changes. Kaas relies
/// on the constant staying constant; a re-shard path
/// is the documented follow-up on gh #174 for
/// the day the value needs to change.
pub const DEFAULT_NUM_SLOTS: usize = 50;

// gh #174: compile-time guard. If a future edit changes the
// constant, the build fails — the on-disk layout is shared with
// existing deployments and silently re-slotting their entries on a
// rolling upgrade breaks the gh #22 fence-on-rejoin contract.
// Bump this assertion deliberately as part of the same change that
// ships the migration path.
const _: () = assert!(
    DEFAULT_NUM_SLOTS == 50,
    "NUM_SLOTS is shared with on-disk layout — see gh #174"
);

/// Transaction state machine. Mirrors Apache's `TransactionState`
/// (TransactionMetadata.scala). The on-disk JSON representation
/// uses stable human-readable strings (v0.1-compatible).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum TxnState {
    /// No transaction in progress (default).
    #[serde(rename = "")]
    #[default]
    Empty,
    /// At least one `AddPartitionsToTxn` or `AddOffsetsToTxn`
    /// succeeded.
    Ongoing,
    /// `EndTxn(commit)` accepted, transition in flight.
    PrepareCommit,
    /// `EndTxn(abort)` accepted, transition in flight.
    PrepareAbort,
    /// Commit finished. Idempotent commit retries return `Ok(())`.
    CompleteCommit,
    /// Abort finished. Idempotent abort retries return `Ok(())`.
    CompleteAbort,
}

impl TxnState {
    fn is_empty(&self) -> bool {
        matches!(self, TxnState::Empty)
    }
}

/// One `(topic, partitions)` tuple inside a [`TxnEntry`]. Apache's
/// wire/storage shape uses `TopicPartition` (a single `(topic, int32)`
/// pair) but groups by topic on the wire; we store the same grouped
/// form.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TxnTopic {
    pub topic: String,
    pub partitions: Vec<i32>,
}

/// Persistent record of one transactional producer. JSON shape is
/// pinned so a v0.1-written slot file
/// reads cleanly through this struct (and vice versa for the
/// migration window).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TxnEntry {
    pub pid: i64,
    pub epoch: i16,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub partitions: Vec<TxnTopic>,
    /// Wall-clock millis when the entry entered `Ongoing`. Zero
    /// in any other state. Reaper input.
    #[serde(default, skip_serializing_if = "i64_is_zero")]
    pub ongoing_since_ms: i64,
    /// Mirrors `InitProducerIdRequest.transaction_timeout_ms`
    /// (KIP-98).
    #[serde(default, skip_serializing_if = "i32_is_zero")]
    pub transaction_timeout_ms: i32,
    #[serde(default, skip_serializing_if = "TxnState::is_empty")]
    pub state: TxnState,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub groups: Vec<String>,
}

fn i64_is_zero(v: &i64) -> bool {
    *v == 0
}
fn i32_is_zero(v: &i32) -> bool {
    *v == 0
}

/// One transaction still owing markers, as reported by
/// [`TxnStateStore::pending_marker_dispatches`]. `epoch` is the
/// entry's *current* epoch — for a reaper-prepared abort that is the
/// already-bumped one, and it is what
/// [`TxnStateStore::complete_end_txn`] validates against.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingMarkerDispatch {
    pub txn_id: String,
    pub pid: i64,
    pub epoch: i16,
    /// `true` for COMMIT, `false` for ABORT.
    pub commit: bool,
    pub partitions: Vec<TxnTopic>,
}

/// Side-effect record [`TxnStateStore::abort_overdue_owned`] returns
/// per aborted txn — feeds metrics, the future cross-broker marker
/// writer (gh #114), and tests.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxnAbortRecord {
    pub txn_id: String,
    pub pid: i64,
    pub old_epoch: i16,
    pub new_epoch: i16,
    pub groups: Vec<String>,
}

/// Return value of [`TxnStateStore::prepare_end_txn`]. Carries the
/// partition + group lists the caller must dispatch COMMIT / ABORT
/// control batches for (gh #114 / gh #175).
///
/// The lists are *copies* — they stay on the persisted entry until
/// [`TxnStateStore::complete_end_txn`] clears them, so a dispatch
/// failure or a coordinator crash re-derives the identical set on the
/// next attempt (gh #225).
///
/// `transition_fired = false` is the nothing-to-do path — the txn was
/// already `CompleteCommit`/`CompleteAbort`, its markers are long
/// written, and no fresh side effects are required.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct EndTxnOutcome {
    pub partitions: Vec<TxnTopic>,
    pub groups: Vec<String>,
    pub transition_fired: bool,
}

/// Cross-coordinator signal that fires on every `EndTxn` (and
/// reaper-driven abort) transition. The txn coordinator tells the
/// group's offset store to either materialise (`commit = true`) or
/// discard (`commit = false`) the pending offsets that
/// `TxnOffsetCommit` staged earlier.
///
/// In Apache, this signal travels via `WriteTxnMarkers` to the
/// `__consumer_offsets[partitionFor(group_id)]` partition's leader.
/// Kaas stages it as a local hook — when txn coord and group
/// coord live on the same broker it fires directly; cross-broker
/// dispatch lands with gh #114.
pub trait TxnOffsetHook: Send + Sync + 'static {
    fn on_end_txn(&self, group_id: &str, producer_id: i64, commit: bool);
}

/// Errors mappable to Kafka wire codes by the txn handlers. Keeping
/// the store transport-free lets each handler pick the v0-3 (per-
/// partition error code) or v4+ (top-level error code) shape
/// without leaking codec types into this crate.
#[derive(Debug, Error)]
pub enum TxnStateError {
    /// Wire code 71 — INVALID_TRANSACTIONAL_ID.
    #[error("txn state: empty transactional id")]
    EmptyTxnId,
    /// Wire code 49 — UNKNOWN_PRODUCER_ID.
    #[error("txn state: unknown txn id or pid mismatch")]
    UnknownProducer,
    /// Wire code 47 — INVALID_PRODUCER_EPOCH.
    #[error("txn state: producer epoch fenced")]
    EpochFenced,
    /// Wire code 51 — CONCURRENT_TRANSACTIONS.
    #[error("txn state: concurrent transition in progress")]
    Concurrent,
    /// Wire code 50 — INVALID_TXN_STATE.
    #[error("txn state: invalid state transition")]
    InvalidState,
    #[error("txn state: i/o: {0}")]
    Io(#[from] io::Error),
    #[error("txn state: decode: {0}")]
    Decode(#[from] serde_json::Error),
}

type Result<T> = std::result::Result<T, TxnStateError>;

/// Per-cluster transactional-state store. See module docs.
pub struct TxnStateStore {
    dir: PathBuf,
    num_slots: usize,
    /// Single global mutex serialises slot reads + writes
    /// (coarse on purpose). The store sits off the hot
    /// path (txn surface fires at producer boot + per-txn commit;
    /// Produce/Fetch never touch it) so coarse locking is fine.
    mu: Mutex<()>,
    hook: RwLock<Option<Arc<dyn TxnOffsetHook>>>,
}

impl std::fmt::Debug for TxnStateStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TxnStateStore")
            .field("dir", &self.dir)
            .field("num_slots", &self.num_slots)
            .finish()
    }
}

impl TxnStateStore {
    /// Open the per-cluster transactional-state dir under
    /// `parent_dir/txn_state/`. `num_slots == 0` falls back to
    /// [`DEFAULT_NUM_SLOTS`]. Creates the directory if missing.
    pub fn open(parent_dir: &Path, num_slots: usize) -> io::Result<Self> {
        let num_slots = if num_slots == 0 {
            DEFAULT_NUM_SLOTS
        } else {
            num_slots
        };
        let dir = parent_dir.join("txn_state");
        fs::create_dir_all(&dir)?;
        Ok(Self {
            dir,
            num_slots,
            mu: Mutex::new(()),
            hook: RwLock::new(None),
        })
    }

    pub fn num_slots(&self) -> usize {
        self.num_slots
    }

    /// Wire the cross-coordinator offset hook. Production sets this
    /// from the broker bootstrap so `EndTxn` / reaper aborts
    /// materialise or discard pending offsets staged by
    /// `TxnOffsetCommit`. `None` (the default) leaves the hook
    /// silent — pending offsets remain staged.
    pub fn set_offset_hook(&self, hook: Arc<dyn TxnOffsetHook>) {
        *self.hook.write() = Some(hook);
    }

    /// The gh #22 contract: first call for `txn_id` returns a fresh
    /// PID with `epoch = 0`; every subsequent call returns the
    /// **same** PID with `epoch += 1`.
    pub fn get_or_allocate<F>(&self, txn_id: &str, alloc: F) -> Result<(i64, i16)>
    where
        F: FnOnce() -> i64,
    {
        self.get_or_allocate_with_timeout(txn_id, 0, alloc)
    }

    /// As [`get_or_allocate`] but also records the producer's
    /// `transaction.timeout.ms`. `timeout_ms <= 0` leaves the
    /// existing entry's timeout untouched (cooperative for the
    /// non-transactional fast path).
    pub fn get_or_allocate_with_timeout<F>(
        &self,
        txn_id: &str,
        timeout_ms: i32,
        alloc: F,
    ) -> Result<(i64, i16)>
    where
        F: FnOnce() -> i64,
    {
        if txn_id.is_empty() {
            return Err(TxnStateError::EmptyTxnId);
        }
        let _guard = self.mu.lock();
        let slot = self.slot_for(txn_id);
        let mut state = self.load_slot(slot)?;

        let mut entry = match state.get(txn_id) {
            None => TxnEntry {
                pid: alloc(),
                epoch: 0,
                partitions: Vec::new(),
                ongoing_since_ms: 0,
                transaction_timeout_ms: 0,
                state: TxnState::Empty,
                groups: Vec::new(),
            },
            Some(existing) if existing.epoch == i16::MAX => TxnEntry {
                pid: alloc(),
                epoch: 0,
                partitions: Vec::new(),
                ongoing_since_ms: 0,
                transaction_timeout_ms: existing.transaction_timeout_ms,
                state: TxnState::Empty,
                groups: Vec::new(),
            },
            Some(existing) => {
                let mut e = existing.clone();
                e.epoch += 1;
                e
            }
        };
        if timeout_ms > 0 {
            entry.transaction_timeout_ms = timeout_ms;
        }
        let pid = entry.pid;
        let epoch = entry.epoch;
        state.insert(txn_id.to_owned(), entry);
        self.persist_slot(slot, &state)?;
        Ok((pid, epoch))
    }

    /// Union `additions` into the entry's partition list. gh #23.
    /// Validation order matches Apache:
    /// 1. empty `txn_id` → [`TxnStateError::EmptyTxnId`]
    /// 2. no entry → [`TxnStateError::UnknownProducer`]
    /// 3. PID mismatch → [`TxnStateError::UnknownProducer`]
    /// 4. Epoch mismatch → [`TxnStateError::EpochFenced`]
    /// 5. `Prepare*` state → [`TxnStateError::Concurrent`]
    pub fn add_partitions(
        &self,
        txn_id: &str,
        pid: i64,
        epoch: i16,
        additions: &[TxnTopic],
        now_ms: i64,
    ) -> Result<()> {
        if txn_id.is_empty() {
            return Err(TxnStateError::EmptyTxnId);
        }
        let _guard = self.mu.lock();
        let slot = self.slot_for(txn_id);
        let mut state = self.load_slot(slot)?;

        let mut entry = state
            .get(txn_id)
            .cloned()
            .ok_or(TxnStateError::UnknownProducer)?;
        if entry.pid != pid {
            return Err(TxnStateError::UnknownProducer);
        }
        if entry.epoch != epoch {
            return Err(TxnStateError::EpochFenced);
        }
        match entry.state {
            TxnState::PrepareCommit | TxnState::PrepareAbort => {
                return Err(TxnStateError::Concurrent);
            }
            _ => {}
        }

        let merged = merge_partitions(&mut entry, additions);
        let was_not_ongoing = entry.state != TxnState::Ongoing;
        if was_not_ongoing {
            entry.state = TxnState::Ongoing;
            entry.ongoing_since_ms = now_ms;
        }
        if !merged && !was_not_ongoing {
            // Idempotent no-op — every (topic, partition) already
            // recorded AND no state change.
            return Ok(());
        }
        state.insert(txn_id.to_owned(), entry);
        self.persist_slot(slot, &state)?;
        Ok(())
    }

    /// Record that the producer will commit offsets to consumer
    /// group `group_id` as part of this transaction. gh #24.
    /// Idempotent — re-adding the same group is a no-op.
    pub fn add_offsets_to_txn(
        &self,
        txn_id: &str,
        pid: i64,
        epoch: i16,
        group_id: &str,
        now_ms: i64,
    ) -> Result<()> {
        if txn_id.is_empty() {
            return Err(TxnStateError::EmptyTxnId);
        }
        if group_id.is_empty() {
            return Err(TxnStateError::InvalidState);
        }
        let _guard = self.mu.lock();
        let slot = self.slot_for(txn_id);
        let mut state = self.load_slot(slot)?;

        let mut entry = state
            .get(txn_id)
            .cloned()
            .ok_or(TxnStateError::UnknownProducer)?;
        if entry.pid != pid {
            return Err(TxnStateError::UnknownProducer);
        }
        if entry.epoch != epoch {
            return Err(TxnStateError::EpochFenced);
        }
        match entry.state {
            TxnState::PrepareCommit | TxnState::PrepareAbort => {
                return Err(TxnStateError::Concurrent);
            }
            _ => {}
        }

        let already_recorded = entry.groups.iter().any(|g| g == group_id);
        let needs_state_advance = entry.state != TxnState::Ongoing;
        if already_recorded && !needs_state_advance {
            return Ok(());
        }
        if !already_recorded {
            entry.groups.push(group_id.to_owned());
        }
        if needs_state_advance {
            entry.state = TxnState::Ongoing;
            entry.ongoing_since_ms = now_ms;
        }
        state.insert(txn_id.to_owned(), entry);
        self.persist_slot(slot, &state)?;
        Ok(())
    }

    /// Phase 1 of the `EndTxn` (API key 26) transition. gh #25 / #26,
    /// split out under gh #225.
    ///
    /// ```text
    /// Ongoing        → PrepareCommit   (commit = true)
    /// Ongoing        → PrepareAbort    (commit = false)
    /// PrepareCommit  + commit = true   → re-drive (same lists)
    /// PrepareAbort   + commit = false  → re-drive (same lists)
    /// PrepareCommit  + commit = false  → InvalidState
    /// PrepareAbort   + commit = true   → InvalidState
    /// CompleteCommit + commit = true   → Ok, transition_fired = false
    /// CompleteAbort  + commit = false  → Ok, transition_fired = false
    /// Complete* mismatched             → InvalidState
    /// Empty                            → InvalidState
    /// ```
    ///
    /// **The partition and group lists are deliberately NOT cleared
    /// here** — that is the whole point of the split. `Prepare*` plus a
    /// retained partition list *is* the durable record of "these
    /// markers still owe a write", so a dispatch failure, a coordinator
    /// crash, or a producer retry all re-derive the same dispatch set
    /// instead of losing it. Clearing happens in
    /// [`Self::complete_end_txn`], which the caller may only invoke once
    /// every marker is durable.
    ///
    /// This is the NFS-substrate rule-2 shape (see
    /// `docs/src/architecture/nfs-substrate.md`): the compound
    /// "transition + write N markers" op is not a single atomic
    /// primitive, so it is made idempotent and driven to completion by
    /// retry rather than fired once and hoped for.
    ///
    /// Re-driving on a matching `Prepare*` replaces the previous
    /// `Concurrent` error. A producer retrying `EndTxn` after a failed
    /// dispatch must be able to push it through; answering
    /// CONCURRENT_TRANSACTIONS forever is exactly the wedge gh #225
    /// describes.
    pub fn prepare_end_txn(
        &self,
        txn_id: &str,
        pid: i64,
        epoch: i16,
        commit: bool,
    ) -> Result<EndTxnOutcome> {
        if txn_id.is_empty() {
            return Err(TxnStateError::EmptyTxnId);
        }
        let _guard = self.mu.lock();
        let slot = self.slot_for(txn_id);
        let mut state = self.load_slot(slot)?;

        let mut entry = state
            .get(txn_id)
            .cloned()
            .ok_or(TxnStateError::UnknownProducer)?;
        if entry.pid != pid {
            return Err(TxnStateError::UnknownProducer);
        }
        if entry.epoch != epoch {
            return Err(TxnStateError::EpochFenced);
        }

        let want_prepare = if commit {
            TxnState::PrepareCommit
        } else {
            TxnState::PrepareAbort
        };

        match entry.state {
            TxnState::Ongoing => {
                entry.state = want_prepare;
                // Lists stay put — see the doc comment. They are read
                // out for the caller but remain on the entry so a
                // re-drive sees the same set.
                let partitions = entry.partitions.clone();
                let groups = entry.groups.clone();
                state.insert(txn_id.to_owned(), entry);
                self.persist_slot(slot, &state)?;
                Ok(EndTxnOutcome {
                    partitions,
                    groups,
                    transition_fired: true,
                })
            }
            // Already preparing: re-drive with the retained lists so a
            // retry re-attempts dispatch for exactly the same targets.
            TxnState::PrepareCommit | TxnState::PrepareAbort => {
                if entry.state != want_prepare {
                    return Err(TxnStateError::InvalidState);
                }
                Ok(EndTxnOutcome {
                    partitions: entry.partitions.clone(),
                    groups: entry.groups.clone(),
                    transition_fired: true,
                })
            }
            TxnState::CompleteCommit => {
                if commit {
                    Ok(EndTxnOutcome::default())
                } else {
                    Err(TxnStateError::InvalidState)
                }
            }
            TxnState::CompleteAbort => {
                if commit {
                    Err(TxnStateError::InvalidState)
                } else {
                    Ok(EndTxnOutcome::default())
                }
            }
            TxnState::Empty => Err(TxnStateError::InvalidState),
        }
    }

    /// Phase 2 of `EndTxn`: `Prepare* → Complete*`, clearing the
    /// partition + group lists and firing the offset hook.
    ///
    /// **Only call this once every marker for the transaction is
    /// durable.** It is the step that discards the dispatch set, so
    /// calling it early re-creates gh #225: the txn reads as finished
    /// while some partition never received its marker, leaving
    /// `read_committed` consumers with a permanently pinned LSO and no
    /// way for any retry to notice.
    ///
    /// The offset hook fires here rather than in
    /// [`Self::prepare_end_txn`] so staged `TxnOffsetCommit` offsets
    /// only become visible once the markers backing them are durable.
    ///
    /// Idempotent on `Complete*` with a matching `commit`.
    pub fn complete_end_txn(&self, txn_id: &str, pid: i64, epoch: i16, commit: bool) -> Result<()> {
        if txn_id.is_empty() {
            return Err(TxnStateError::EmptyTxnId);
        }
        let _guard = self.mu.lock();
        let slot = self.slot_for(txn_id);
        let mut state = self.load_slot(slot)?;

        let mut entry = state
            .get(txn_id)
            .cloned()
            .ok_or(TxnStateError::UnknownProducer)?;
        if entry.pid != pid {
            return Err(TxnStateError::UnknownProducer);
        }
        if entry.epoch != epoch {
            return Err(TxnStateError::EpochFenced);
        }

        let (want_prepare, want_complete) = if commit {
            (TxnState::PrepareCommit, TxnState::CompleteCommit)
        } else {
            (TxnState::PrepareAbort, TxnState::CompleteAbort)
        };

        if entry.state == want_complete {
            return Ok(()); // idempotent
        }
        if entry.state != want_prepare {
            return Err(TxnStateError::InvalidState);
        }

        entry.state = want_complete;
        let groups = std::mem::take(&mut entry.groups);
        entry.partitions.clear();
        entry.ongoing_since_ms = 0;
        state.insert(txn_id.to_owned(), entry);
        self.persist_slot(slot, &state)?;
        let hook = self.hook.read().clone();
        if let Some(hook) = hook {
            for g in &groups {
                hook.on_end_txn(g, pid, commit);
            }
        }
        Ok(())
    }

    /// Single-phase `EndTxn` — [`Self::prepare_end_txn`] immediately
    /// followed by [`Self::complete_end_txn`].
    ///
    /// **Not for the production EndTxn path.** It completes the
    /// transaction without giving the caller a window to write markers,
    /// which is precisely the gh #225 defect. It exists for tests and
    /// for callers that have no markers to dispatch.
    pub fn end_txn(
        &self,
        txn_id: &str,
        pid: i64,
        epoch: i16,
        commit: bool,
    ) -> Result<EndTxnOutcome> {
        let outcome = self.prepare_end_txn(txn_id, pid, epoch, commit)?;
        if outcome.transition_fired {
            self.complete_end_txn(txn_id, pid, epoch, commit)?;
        }
        Ok(outcome)
    }

    /// As [`abort_overdue_owned`] without the ownership gate.
    /// Tests / dev mode only — production multi-broker setups must
    /// pass a real `owns_txn` closure.
    pub fn abort_overdue(&self, now_ms: i64) -> Vec<TxnAbortRecord> {
        self.abort_overdue_owned(now_ms, None)
    }

    /// Every transaction sitting in `Prepare*` with partitions still
    /// owed a COMMIT / ABORT marker (gh #225).
    ///
    /// This is the reconcile half of NFS-substrate rule 2. The retry
    /// half — a producer re-sending `EndTxn` — only covers producers
    /// that come back. A producer that crashed, gave up, or was fenced
    /// leaves the transaction prepared forever, and every timed-out
    /// transaction the reaper prepares has no producer to retry at all.
    /// Something has to drive those to completion, or their partitions
    /// keep a pinned LSO indefinitely.
    ///
    /// Entries with an empty partition list are skipped: there is
    /// nothing to dispatch, and the caller's `complete_end_txn` closes
    /// them out on the next pass anyway.
    ///
    /// `owns_txn` gates the sweep exactly as
    /// [`Self::abort_overdue_owned`] does — a slot file has one legal
    /// writer (NFS substrate rule 3), so a multi-broker deployment must
    /// pass a real closure.
    /// The `Sync` bound (unlike [`Self::abort_overdue_owned`]'s) is
    /// what lets an async caller hold the closure across an await and
    /// still have a `Send` future — the marker reconcile drives this
    /// from a spawned task.
    pub fn pending_marker_dispatches(
        &self,
        owns_txn: Option<&(dyn Fn(&str) -> bool + Sync)>,
    ) -> Vec<PendingMarkerDispatch> {
        let _guard = self.mu.lock();
        let mut out = Vec::new();
        for slot in 0..self.num_slots {
            let state = match self.load_slot(slot) {
                Ok(s) => s,
                Err(_) => continue,
            };
            for (txn_id, entry) in state {
                let commit = match entry.state {
                    TxnState::PrepareCommit => true,
                    TxnState::PrepareAbort => false,
                    _ => continue,
                };
                if entry.partitions.is_empty() {
                    continue;
                }
                if let Some(owns) = owns_txn {
                    if !owns(&txn_id) {
                        continue;
                    }
                }
                out.push(PendingMarkerDispatch {
                    txn_id,
                    pid: entry.pid,
                    epoch: entry.epoch,
                    commit,
                    partitions: entry.partitions,
                });
            }
        }
        out
    }

    /// Walk every slot, abort `Ongoing` entries past their
    /// `ongoing_since_ms + transaction_timeout_ms` deadline. gh #28.
    /// Bumps the producer epoch on abort so the next `InitProducerId`
    /// from the stuck client fences out via the gh #22 path.
    ///
    /// Lands the entry in **`PrepareAbort`, not `CompleteAbort`**, with
    /// its partition and group lists intact (gh #225). A timed-out
    /// transaction owes its partitions an ABORT marker exactly like a
    /// client-driven abort does; completing it here would clear the
    /// dispatch set with nothing written, leaving `read_committed`
    /// consumers with a pinned LSO and no record that anything was
    /// owed. [`Self::pending_marker_dispatches`] is what picks these up.
    ///
    /// The offset hook therefore fires in
    /// [`Self::complete_end_txn`] rather than here, so staged offsets
    /// are discarded on the same edge that writes the markers.
    ///
    /// `owns_txn` gates the sweep: when `Some`, only entries this
    /// broker is the coordinator for are touched (gh #91). When
    /// `None` (tests / dev / single broker) every slot is in scope.
    pub fn abort_overdue_owned(
        &self,
        now_ms: i64,
        owns_txn: Option<&dyn Fn(&str) -> bool>,
    ) -> Vec<TxnAbortRecord> {
        let _guard = self.mu.lock();
        let mut aborted = Vec::new();
        for slot in 0..self.num_slots {
            let mut state = match self.load_slot(slot) {
                Ok(s) => s,
                Err(_) => continue,
            };
            let mut changed = false;
            // Collect txn_ids first to avoid concurrent mutation
            // of the map while iterating.
            let candidate_ids: Vec<String> = state.keys().cloned().collect();
            for txn_id in candidate_ids {
                let entry = match state.get(&txn_id) {
                    Some(e) if e.state == TxnState::Ongoing => e.clone(),
                    _ => continue,
                };
                if let Some(owns) = owns_txn {
                    if !owns(&txn_id) {
                        continue;
                    }
                }
                if entry.ongoing_since_ms == 0 || entry.transaction_timeout_ms <= 0 {
                    continue;
                }
                let deadline = entry.ongoing_since_ms + i64::from(entry.transaction_timeout_ms);
                if deadline > now_ms {
                    continue;
                }

                let pid = entry.pid;
                let old_epoch = entry.epoch;
                let groups = entry.groups.clone();

                let mut updated = entry;
                // PrepareAbort, not CompleteAbort: the partition and
                // group lists stay so the marker reconcile can find
                // them (gh #225). `complete_end_txn` clears them and
                // fires the offset hook once the markers are durable.
                updated.state = TxnState::PrepareAbort;
                updated.ongoing_since_ms = 0;
                updated.epoch = if updated.epoch == i16::MAX {
                    0
                } else {
                    updated.epoch + 1
                };
                let new_epoch = updated.epoch;
                state.insert(txn_id.clone(), updated);
                changed = true;

                aborted.push(TxnAbortRecord {
                    txn_id,
                    pid,
                    old_epoch,
                    new_epoch,
                    groups,
                });
            }
            if changed {
                let _ = self.persist_slot(slot, &state);
            }
        }
        aborted
    }

    /// Copy of every txn entry across every slot. Tests only.
    pub fn snapshot(&self) -> HashMap<String, TxnEntry> {
        let _guard = self.mu.lock();
        let mut out = HashMap::new();
        for slot in 0..self.num_slots {
            if let Ok(state) = self.load_slot(slot) {
                out.extend(state);
            }
        }
        out
    }

    fn slot_for(&self, txn_id: &str) -> usize {
        // Same dance as `kaas-broker::group_hash::coordinator_slot` —
        // u32 → u64 → usize is safe on every target (usize ≥ 32 bits)
        // and dodges the workspace `clippy::as-conversions` lint.
        let h = u64::from(fnv1a_32(txn_id.as_bytes()));
        let n = u64::try_from(self.num_slots).unwrap_or(u64::MAX);
        usize::try_from(h % n).unwrap_or(0)
    }

    fn slot_path(&self, slot: usize) -> PathBuf {
        self.dir.join(format!("slot-{slot}.json"))
    }

    fn load_slot(&self, slot: usize) -> Result<HashMap<String, TxnEntry>> {
        let path = self.slot_path(slot);
        let data = match fs::read(&path) {
            Ok(d) => d,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(HashMap::new()),
            Err(e) => return Err(e.into()),
        };
        if data.is_empty() {
            return Ok(HashMap::new());
        }
        let state: HashMap<String, TxnEntry> = serde_json::from_slice(&data)?;
        Ok(state)
    }

    fn persist_slot(&self, slot: usize, state: &HashMap<String, TxnEntry>) -> Result<()> {
        let name = format!("slot-{slot}.json");
        atomic_write_json(&self.dir, &name, state)?;
        Ok(())
    }
}

/// Union `additions` into `entry.partitions` in place. Returns
/// `true` if anything new was added; `false` if every
/// `(topic, partition)` was already recorded.
fn merge_partitions(entry: &mut TxnEntry, additions: &[TxnTopic]) -> bool {
    let mut changed = false;
    for add in additions {
        if let Some(existing) = entry.partitions.iter_mut().find(|t| t.topic == add.topic) {
            for p in &add.partitions {
                if !existing.partitions.contains(p) {
                    existing.partitions.push(*p);
                    changed = true;
                }
            }
        } else {
            entry.partitions.push(TxnTopic {
                topic: add.topic.clone(),
                partitions: add.partitions.clone(),
            });
            changed = true;
        }
    }
    changed
}

/// FNV-1a 32-bit. Same algorithm as `crates/kaas-broker/src/group_hash.rs`;
/// inlined here so `kaas-coordinator` doesn't pull
/// in a fnv crate just for the slot hash.
fn fnv1a_32(bytes: &[u8]) -> u32 {
    const OFFSET: u32 = 0x811c_9dc5;
    const PRIME: u32 = 0x0100_0193;
    let mut h = OFFSET;
    for b in bytes {
        h ^= u32::from(*b);
        h = h.wrapping_mul(PRIME);
    }
    h
}

#[cfg(test)]
#[allow(clippy::redundant_closure)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicI64, Ordering};

    fn store() -> (tempfile::TempDir, TxnStateStore) {
        let tmp = tempfile::tempdir().unwrap();
        let store = TxnStateStore::open(tmp.path(), DEFAULT_NUM_SLOTS).unwrap();
        (tmp, store)
    }

    fn pid_alloc() -> impl FnMut() -> i64 {
        let counter = AtomicI64::new(100);
        move || counter.fetch_add(1, Ordering::SeqCst)
    }

    #[test]
    fn empty_txn_id_rejected() {
        let (_t, s) = store();
        let mut a = pid_alloc();
        assert!(matches!(
            s.get_or_allocate("", || a()),
            Err(TxnStateError::EmptyTxnId)
        ));
    }

    #[test]
    fn first_call_allocates_epoch_zero_rejoin_bumps() {
        let (_t, s) = store();
        let mut a = pid_alloc();
        let (pid1, e1) = s.get_or_allocate("tx-1", || a()).unwrap();
        assert_eq!(e1, 0);
        let (pid2, e2) = s.get_or_allocate("tx-1", || a()).unwrap();
        assert_eq!(pid1, pid2);
        assert_eq!(e2, 1);
        let (pid3, e3) = s.get_or_allocate("tx-1", || a()).unwrap();
        assert_eq!(pid1, pid3);
        assert_eq!(e3, 2);
    }

    #[test]
    fn distinct_txn_ids_get_distinct_pids() {
        let (_t, s) = store();
        let mut a = pid_alloc();
        let (p1, _) = s.get_or_allocate("tx-a", || a()).unwrap();
        let (p2, _) = s.get_or_allocate("tx-b", || a()).unwrap();
        assert_ne!(p1, p2);
    }

    #[test]
    fn add_partitions_unknown_producer() {
        let (_t, s) = store();
        let err = s
            .add_partitions(
                "tx-1",
                1,
                0,
                &[TxnTopic {
                    topic: "t".into(),
                    partitions: vec![0],
                }],
                0,
            )
            .unwrap_err();
        assert!(matches!(err, TxnStateError::UnknownProducer));
    }

    #[test]
    fn add_partitions_happy_path_then_idempotent() {
        let (_t, s) = store();
        let mut a = pid_alloc();
        let (pid, epoch) = s.get_or_allocate("tx-1", || a()).unwrap();
        s.add_partitions(
            "tx-1",
            pid,
            epoch,
            &[TxnTopic {
                topic: "t".into(),
                partitions: vec![0, 1],
            }],
            1_000,
        )
        .unwrap();
        // Idempotent re-add — same tuples, no error, no spurious write.
        s.add_partitions(
            "tx-1",
            pid,
            epoch,
            &[TxnTopic {
                topic: "t".into(),
                partitions: vec![0],
            }],
            2_000,
        )
        .unwrap();
        let snap = s.snapshot();
        let entry = &snap["tx-1"];
        assert_eq!(entry.state, TxnState::Ongoing);
        assert_eq!(
            entry.partitions,
            vec![TxnTopic {
                topic: "t".into(),
                partitions: vec![0, 1]
            }]
        );
        assert_eq!(entry.ongoing_since_ms, 1_000);
    }

    #[test]
    fn add_partitions_unions_across_calls() {
        let (_t, s) = store();
        let mut a = pid_alloc();
        let (pid, epoch) = s.get_or_allocate("tx-1", || a()).unwrap();
        s.add_partitions(
            "tx-1",
            pid,
            epoch,
            &[TxnTopic {
                topic: "t".into(),
                partitions: vec![0],
            }],
            10,
        )
        .unwrap();
        s.add_partitions(
            "tx-1",
            pid,
            epoch,
            &[
                TxnTopic {
                    topic: "t".into(),
                    partitions: vec![1],
                },
                TxnTopic {
                    topic: "u".into(),
                    partitions: vec![5],
                },
            ],
            20,
        )
        .unwrap();
        let snap = s.snapshot();
        let entry = &snap["tx-1"];
        assert_eq!(
            entry.partitions,
            vec![
                TxnTopic {
                    topic: "t".into(),
                    partitions: vec![0, 1]
                },
                TxnTopic {
                    topic: "u".into(),
                    partitions: vec![5]
                },
            ]
        );
    }

    #[test]
    fn epoch_mismatch_fences() {
        let (_t, s) = store();
        let mut a = pid_alloc();
        let (pid, _) = s.get_or_allocate("tx-1", || a()).unwrap();
        let err = s
            .add_partitions(
                "tx-1",
                pid,
                7, // wrong epoch
                &[TxnTopic {
                    topic: "t".into(),
                    partitions: vec![0],
                }],
                0,
            )
            .unwrap_err();
        assert!(matches!(err, TxnStateError::EpochFenced));
    }

    /// gh #225: `prepare_end_txn` must leave the dispatch set on the
    /// persisted entry. It is the only durable record that these
    /// markers still owe a write — clearing it early is what made a
    /// failed dispatch unrecoverable.
    #[test]
    fn prepare_retains_the_dispatch_set_until_complete() {
        let (_t, s) = store();
        let mut a = pid_alloc();
        let (pid, epoch) = s.get_or_allocate("tx-1", || a()).unwrap();
        s.add_partitions(
            "tx-1",
            pid,
            epoch,
            &[TxnTopic {
                topic: "t".into(),
                partitions: vec![0, 1],
            }],
            100,
        )
        .unwrap();

        let out = s.prepare_end_txn("tx-1", pid, epoch, true).unwrap();
        assert!(out.transition_fired);
        assert_eq!(out.partitions.len(), 1);
        let snap = s.snapshot();
        assert_eq!(snap["tx-1"].state, TxnState::PrepareCommit);
        assert_eq!(
            snap["tx-1"].partitions.len(),
            1,
            "dispatch set must survive prepare"
        );

        s.complete_end_txn("tx-1", pid, epoch, true).unwrap();
        let snap = s.snapshot();
        assert_eq!(snap["tx-1"].state, TxnState::CompleteCommit);
        assert!(snap["tx-1"].partitions.is_empty());
    }

    /// A retry after a failed dispatch must re-derive the *same*
    /// targets, not an empty set.
    #[test]
    fn prepare_is_re_drivable_with_the_same_dispatch_set() {
        let (_t, s) = store();
        let mut a = pid_alloc();
        let (pid, epoch) = s.get_or_allocate("tx-1", || a()).unwrap();
        s.add_partitions(
            "tx-1",
            pid,
            epoch,
            &[TxnTopic {
                topic: "t".into(),
                partitions: vec![0],
            }],
            100,
        )
        .unwrap();
        let first = s.prepare_end_txn("tx-1", pid, epoch, true).unwrap();
        let second = s.prepare_end_txn("tx-1", pid, epoch, true).unwrap();
        assert!(second.transition_fired);
        assert_eq!(first.partitions, second.partitions);
    }

    /// Prepared-to-commit cannot be flipped to an abort (and vice
    /// versa) — that would write the opposite marker for a txn whose
    /// COMMIT may already be on disk at some partition.
    #[test]
    fn prepare_rejects_the_opposite_outcome() {
        let (_t, s) = store();
        let mut a = pid_alloc();
        let (pid, epoch) = s.get_or_allocate("tx-1", || a()).unwrap();
        s.add_partitions(
            "tx-1",
            pid,
            epoch,
            &[TxnTopic {
                topic: "t".into(),
                partitions: vec![0],
            }],
            100,
        )
        .unwrap();
        s.prepare_end_txn("tx-1", pid, epoch, true).unwrap();
        assert!(matches!(
            s.prepare_end_txn("tx-1", pid, epoch, false),
            Err(TxnStateError::InvalidState)
        ));
        assert!(matches!(
            s.complete_end_txn("tx-1", pid, epoch, false),
            Err(TxnStateError::InvalidState)
        ));
    }

    /// Staged offsets must not become visible until the markers
    /// backing them are durable, so the hook fires on complete.
    #[test]
    fn offset_hook_fires_on_complete_not_on_prepare() {
        struct CapturingHook(parking_lot::Mutex<Vec<(String, i64, bool)>>);
        impl TxnOffsetHook for CapturingHook {
            fn on_end_txn(&self, group_id: &str, producer_id: i64, commit: bool) {
                self.0
                    .lock()
                    .push((group_id.to_owned(), producer_id, commit));
            }
        }
        let (_t, s) = store();
        let hook = Arc::new(CapturingHook(parking_lot::Mutex::new(Vec::new())));
        s.set_offset_hook(hook.clone());
        let mut a = pid_alloc();
        let (pid, epoch) = s.get_or_allocate("tx-1", || a()).unwrap();
        s.add_offsets_to_txn("tx-1", pid, epoch, "g1", 100).unwrap();

        s.prepare_end_txn("tx-1", pid, epoch, true).unwrap();
        assert!(
            hook.0.lock().is_empty(),
            "prepare must not publish staged offsets"
        );

        s.complete_end_txn("tx-1", pid, epoch, true).unwrap();
        assert_eq!(hook.0.lock().len(), 1);
        assert_eq!(hook.0.lock()[0], ("g1".to_owned(), pid, true));
    }

    #[test]
    fn complete_without_prepare_is_invalid() {
        let (_t, s) = store();
        let mut a = pid_alloc();
        let (pid, epoch) = s.get_or_allocate("tx-1", || a()).unwrap();
        s.add_partitions(
            "tx-1",
            pid,
            epoch,
            &[TxnTopic {
                topic: "t".into(),
                partitions: vec![0],
            }],
            100,
        )
        .unwrap();
        // Still Ongoing — completing without preparing would clear the
        // dispatch set with no marker written.
        assert!(matches!(
            s.complete_end_txn("tx-1", pid, epoch, true),
            Err(TxnStateError::InvalidState)
        ));
    }

    #[test]
    fn end_txn_happy_commit_clears_partitions_and_fires_hook() {
        struct CapturingHook(parking_lot::Mutex<Vec<(String, i64, bool)>>);
        impl TxnOffsetHook for CapturingHook {
            fn on_end_txn(&self, group_id: &str, producer_id: i64, commit: bool) {
                self.0
                    .lock()
                    .push((group_id.to_owned(), producer_id, commit));
            }
        }
        let (_t, s) = store();
        let hook = Arc::new(CapturingHook(parking_lot::Mutex::new(Vec::new())));
        s.set_offset_hook(hook.clone());
        let mut a = pid_alloc();
        let (pid, epoch) = s.get_or_allocate("tx-1", || a()).unwrap();
        s.add_partitions(
            "tx-1",
            pid,
            epoch,
            &[TxnTopic {
                topic: "t".into(),
                partitions: vec![0],
            }],
            100,
        )
        .unwrap();
        s.add_offsets_to_txn("tx-1", pid, epoch, "g1", 110).unwrap();
        s.add_offsets_to_txn("tx-1", pid, epoch, "g2", 120).unwrap();
        s.end_txn("tx-1", pid, epoch, true).unwrap();

        let snap = s.snapshot();
        let entry = &snap["tx-1"];
        assert_eq!(entry.state, TxnState::CompleteCommit);
        assert!(entry.partitions.is_empty());
        assert!(entry.groups.is_empty());
        assert_eq!(entry.ongoing_since_ms, 0);

        let fired = hook.0.lock().clone();
        assert_eq!(
            fired,
            vec![("g1".to_owned(), pid, true), ("g2".to_owned(), pid, true),]
        );
    }

    #[test]
    fn end_txn_idempotent_retry_returns_ok() {
        let (_t, s) = store();
        let mut a = pid_alloc();
        let (pid, epoch) = s.get_or_allocate("tx-1", || a()).unwrap();
        s.add_partitions(
            "tx-1",
            pid,
            epoch,
            &[TxnTopic {
                topic: "t".into(),
                partitions: vec![0],
            }],
            0,
        )
        .unwrap();
        s.end_txn("tx-1", pid, epoch, true).unwrap();
        s.end_txn("tx-1", pid, epoch, true).unwrap(); // idempotent
        assert!(matches!(
            s.end_txn("tx-1", pid, epoch, false),
            Err(TxnStateError::InvalidState)
        ));
    }

    #[test]
    fn end_txn_against_empty_is_invalid() {
        let (_t, s) = store();
        let mut a = pid_alloc();
        let (pid, epoch) = s.get_or_allocate("tx-1", || a()).unwrap();
        // No AddPartitions / AddOffsets — state stays Empty.
        let err = s.end_txn("tx-1", pid, epoch, true).unwrap_err();
        assert!(matches!(err, TxnStateError::InvalidState));
    }

    #[test]
    fn reaper_aborts_overdue_bumps_epoch_fires_hook() {
        struct CapturingHook(parking_lot::Mutex<Vec<(String, i64, bool)>>);
        impl TxnOffsetHook for CapturingHook {
            fn on_end_txn(&self, group_id: &str, producer_id: i64, commit: bool) {
                self.0
                    .lock()
                    .push((group_id.to_owned(), producer_id, commit));
            }
        }
        let (_t, s) = store();
        let hook = Arc::new(CapturingHook(parking_lot::Mutex::new(Vec::new())));
        s.set_offset_hook(hook.clone());
        let mut a = pid_alloc();
        let (pid, epoch) = s
            .get_or_allocate_with_timeout("tx-1", 1_000, || a())
            .unwrap();
        s.add_partitions(
            "tx-1",
            pid,
            epoch,
            &[TxnTopic {
                topic: "t".into(),
                partitions: vec![0],
            }],
            10_000,
        )
        .unwrap();
        s.add_offsets_to_txn("tx-1", pid, epoch, "g1", 10_000)
            .unwrap();

        // Before the deadline — no abort.
        let aborted = s.abort_overdue(10_500);
        assert!(aborted.is_empty());

        // Past the deadline — single abort, epoch bumped.
        let aborted = s.abort_overdue(20_000);
        assert_eq!(aborted.len(), 1);
        assert_eq!(aborted[0].pid, pid);
        assert_eq!(aborted[0].old_epoch, epoch);
        assert_eq!(aborted[0].new_epoch, epoch + 1);
        assert_eq!(aborted[0].groups, vec!["g1".to_owned()]);

        // gh #225: the reaper leaves the txn in PrepareAbort with its
        // dispatch set intact — a timed-out txn owes ABORT markers just
        // like a client-driven one. Completing here would drop the
        // partition list with nothing written and pin the LSO.
        let snap = s.snapshot();
        let entry = &snap["tx-1"];
        assert_eq!(entry.state, TxnState::PrepareAbort);
        assert_eq!(entry.epoch, epoch + 1);
        assert_eq!(
            entry.partitions,
            vec![TxnTopic {
                topic: "t".into(),
                partitions: vec![0]
            }],
            "dispatch set must survive for the marker reconcile"
        );
        assert!(
            hook.0.lock().is_empty(),
            "offset hook fires on complete, not on the reaper's prepare"
        );

        // The reconcile finds it, and completing discards the staged
        // offsets. Note the *bumped* epoch is what completes it.
        let pending = s.pending_marker_dispatches(None);
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].txn_id, "tx-1");
        assert_eq!(pending[0].pid, pid);
        assert_eq!(pending[0].epoch, epoch + 1);
        assert!(!pending[0].commit, "timed-out txn aborts");

        s.complete_end_txn("tx-1", pid, epoch + 1, false).unwrap();
        let snap = s.snapshot();
        assert_eq!(snap["tx-1"].state, TxnState::CompleteAbort);
        assert!(snap["tx-1"].partitions.is_empty());
        let fired = hook.0.lock().clone();
        assert_eq!(fired, vec![("g1".to_owned(), pid, false)]);

        // Nothing left owing once completed.
        assert!(s.pending_marker_dispatches(None).is_empty());
    }

    #[test]
    fn reaper_owns_gate_filters() {
        let (_t, s) = store();
        let mut a = pid_alloc();
        let (pid, epoch) = s
            .get_or_allocate_with_timeout("tx-mine", 1_000, || a())
            .unwrap();
        s.add_partitions(
            "tx-mine",
            pid,
            epoch,
            &[TxnTopic {
                topic: "t".into(),
                partitions: vec![0],
            }],
            5_000,
        )
        .unwrap();
        let (pid2, e2) = s
            .get_or_allocate_with_timeout("tx-theirs", 1_000, || a())
            .unwrap();
        s.add_partitions(
            "tx-theirs",
            pid2,
            e2,
            &[TxnTopic {
                topic: "t".into(),
                partitions: vec![0],
            }],
            5_000,
        )
        .unwrap();

        let mine_only = |id: &str| id == "tx-mine";
        let aborted = s.abort_overdue_owned(10_000, Some(&mine_only));
        assert_eq!(aborted.len(), 1);
        assert_eq!(aborted[0].txn_id, "tx-mine");
    }

    #[test]
    fn slot_hashing_distributes_consistently() {
        let (_t, s) = store();
        // FNV-1a is deterministic — same input → same slot every call.
        assert_eq!(s.slot_for("tx-1"), s.slot_for("tx-1"));
        // Different inputs almost always land in different slots
        // (50 slots, FNV spread is dense enough for a small sample).
        let slots: Vec<usize> = (0..20).map(|i| s.slot_for(&format!("tx-{i}"))).collect();
        let distinct: std::collections::HashSet<_> = slots.iter().copied().collect();
        assert!(
            distinct.len() > 10,
            "expected reasonable spread, got {distinct:?}"
        );
    }

    #[test]
    fn epoch_overflow_rotates_to_fresh_pid() {
        let (_t, s) = store();
        // Seed the slot file by hand at epoch = MAX so the next
        // get_or_allocate triggers rotation.
        {
            let slot = s.slot_for("tx-1");
            let mut state = HashMap::new();
            state.insert(
                "tx-1".to_owned(),
                TxnEntry {
                    pid: 42,
                    epoch: i16::MAX,
                    partitions: vec![],
                    ongoing_since_ms: 0,
                    transaction_timeout_ms: 5_000,
                    state: TxnState::Empty,
                    groups: vec![],
                },
            );
            s.persist_slot(slot, &state).unwrap();
        }
        let mut a = pid_alloc();
        let (pid, epoch) = s.get_or_allocate("tx-1", || a()).unwrap();
        assert_ne!(pid, 42);
        assert_eq!(epoch, 0);
    }

    #[test]
    fn persistence_round_trip_across_open() {
        let tmp = tempfile::tempdir().unwrap();
        let mut a = pid_alloc();
        let pid;
        {
            let s = TxnStateStore::open(tmp.path(), DEFAULT_NUM_SLOTS).unwrap();
            let (p, _e) = s.get_or_allocate("tx-1", || a()).unwrap();
            pid = p;
            s.add_partitions(
                "tx-1",
                p,
                0,
                &[TxnTopic {
                    topic: "t".into(),
                    partitions: vec![0, 1],
                }],
                100,
            )
            .unwrap();
        }
        // Reopen: same on-disk state surfaces.
        let s2 = TxnStateStore::open(tmp.path(), DEFAULT_NUM_SLOTS).unwrap();
        let snap = s2.snapshot();
        let entry = &snap["tx-1"];
        assert_eq!(entry.pid, pid);
        assert_eq!(entry.epoch, 0);
        assert_eq!(entry.state, TxnState::Ongoing);
        assert_eq!(
            entry.partitions,
            vec![TxnTopic {
                topic: "t".into(),
                partitions: vec![0, 1]
            }]
        );
    }

    #[test]
    fn add_partitions_concurrent_transition_rejected() {
        let (_t, s) = store();
        // Seed Prepare* state directly to test the rejection arm.
        let mut a = pid_alloc();
        let (pid, epoch) = s.get_or_allocate("tx-1", || a()).unwrap();
        {
            let slot = s.slot_for("tx-1");
            let mut state = s.load_slot(slot).unwrap();
            let entry = state.get_mut("tx-1").unwrap();
            entry.state = TxnState::PrepareCommit;
            s.persist_slot(slot, &state).unwrap();
        }
        let err = s
            .add_partitions(
                "tx-1",
                pid,
                epoch,
                &[TxnTopic {
                    topic: "t".into(),
                    partitions: vec![0],
                }],
                0,
            )
            .unwrap_err();
        assert!(matches!(err, TxnStateError::Concurrent));
    }
}
