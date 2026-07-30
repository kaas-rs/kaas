//! Per-partition write path: mutex-guarded state, lock-free read snapshot,
//! per-partition committer task.
//!
//! Per-partition write path plus its committer task. One [`Partition`] per
//! `(topic, partition)` tuple; the engine layer ([`crate::engine`]) holds
//! a `DashMap<(String, i32), Arc<Partition>>` and routes calls.
//!
//! # Concurrency model
//!
//! - `Mutex<PartitionInner>` for the **write path**. Append, segment roll,
//!   manifest persist, producer-state mutation — all under this mutex.
//! - `ArcSwap<ReadSnapshot>` for the **read-path observation channel** —
//!   `high_watermark()` / `log_start_offset()` / `epoch()` read without
//!   ever blocking on the mutex. Preserves the gh #134 fix that kept the
//!   OTel gauge alive when a stuck NAS fsync held the partition lock for
//!   the watchdog deadline.
//! - One [`tokio::task`] per partition (the **committer**) that drains
//!   flush requests via an `mpsc::Receiver`. Each cycle runs
//!   `active.sync_log()` under `tokio::time::timeout(FsyncMaxLatency)`,
//!   updates `completed_flush_seq`, and wakes `acks=all` appenders via
//!   [`tokio::sync::Notify`].
//!
//! # Phase 2 initial slice — what's NOT here yet
//!
//! Group-commit (gh #82) — the optimization where the committer fsyncs
//! outside the partition mutex via a cloned log FD — is **not** in this
//! commit. The committer takes the inner mutex for the entire fsync
//! window, which serializes concurrent appenders for the fsync duration.
//! On tmpfs / local SSD that's fine; on NFS it loses the multi-appender
//! throughput multiplier. A follow-up commit on the same gh #156 issue
//! adds the FD-clone trick once the basic Partition is verified
//! correct.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use bytes::Bytes;
use parking_lot::Mutex;
use tokio::sync::{mpsc, Notify};
use tokio::task::JoinHandle;

use crate::errors::StorageError;
use crate::fs::Fs;
use crate::idempotence::{
    self, parse_batch_producer_info, parse_batch_txn_info, BatchProducerInfo, Outcome,
    ProducerEntry,
};
use crate::manifest::{self, Manifest, ReadResult};
use crate::producer_snapshot::{read_producer_snapshot, write_producer_snapshot};
use crate::recovery_checkpoint::{self, RecoveryCheckpoint};
use crate::segment::{self, parse_batch_offsets, ActiveSegment, SegmentMeta};
use crate::txn_index::{AbortedTxn, AbortedTxnIndex, OpenTxnIndex};

/// gh #recovery-checkpoint: the committer writes a fresh recovery
/// checkpoint once the fsynced log has grown this far past the last one.
/// Bounds crash-recovery scan to at most this many bytes; a clean close
/// checkpoints at EOF regardless, so graceful restarts re-scan nothing.
/// 64 MiB keeps checkpoint writes rare (one per 64 MiB of throughput)
/// while capping the worst-case re-scan at ~64 MiB.
const CHECKPOINT_INTERVAL_BYTES: i64 = 64 * 1024 * 1024;

/// Per-partition tuning knobs.
#[derive(Debug, Clone)]
pub struct PartitionConfig {
    /// Roll the active segment at this size. Default 1 GiB matches
    /// Apache's `segment.bytes`.
    pub segment_bytes: u64,
    /// Emit one sparse index entry per N bytes of log. Default 4 KiB
    /// matches Apache's `index.interval.bytes`.
    pub index_interval_bytes: u64,
    /// `log.flush.interval.messages`. Default 1 = honest acks=all on
    /// every batch; raise to amortise fsync cost over more records.
    pub flush_interval_messages: i64,
    /// Watchdog deadline for one log fsync (gh #95). Default 30 s.
    pub fsync_max_latency: Duration,
}

impl Default for PartitionConfig {
    fn default() -> Self {
        Self {
            segment_bytes: 1 << 30,
            index_interval_bytes: 4096,
            flush_interval_messages: 1,
            fsync_max_latency: Duration::from_secs(30),
        }
    }
}

/// Lock-free observation channel. Updated under the partition mutex
/// alongside every HWM / log_start / segment-roll change; readable
/// without taking the mutex.
#[derive(Debug, Clone)]
pub struct ReadSnapshot {
    pub high_water: i64,
    pub log_start: i64,
    pub epoch: i64,
    /// Sorted oldest-first.
    pub closed: Arc<Vec<SegmentMeta>>,
    pub active_meta: SegmentMeta,
}

/// Blocking half of [`Partition::read`] — runs on a `spawn_blocking`
/// thread (gh #209).
///
/// Two things make this O(bytes returned) rather than O(bytes in the
/// partition), and both were missing before gh #209:
///
/// 1. **Segments that cannot contain `effective_start` are skipped**
///    outright. A closed segment covers `[base_offset, next_base)`,
///    where `next_base` is the following segment's base — or the
///    active segment's base for the last closed one.
/// 2. **The offset index picks the start position.** `search_index`
///    returns a file position at or before the target offset; passing
///    `0` instead (the old behaviour) made `read_batches` walk the
///    segment from byte zero. The `.index` files were written and
///    fsynced on the append path and then never read.
fn read_snapshot(
    fs: &dyn Fs,
    snap: &ReadSnapshot,
    start_offset: i64,
    max_bytes: usize,
) -> Result<Bytes, StorageError> {
    let effective_start = start_offset.max(snap.log_start);
    let mut out = bytes::BytesMut::new();

    for (i, seg) in snap.closed.iter().enumerate() {
        if out.len() >= max_bytes {
            break;
        }
        let next_base = snap
            .closed
            .get(i + 1)
            .map_or(snap.active_meta.base_offset, |n| n.base_offset);
        if next_base <= effective_start {
            continue;
        }
        let pos = segment::search_index(fs, &seg.index_path, seg.base_offset, effective_start);
        let chunk = segment::read_batches(
            fs,
            &seg.log_path,
            pos,
            effective_start,
            max_bytes.saturating_sub(out.len()),
        )?;
        out.extend_from_slice(&chunk);
    }

    if out.len() < max_bytes {
        let pos = segment::search_index(
            fs,
            &snap.active_meta.index_path,
            snap.active_meta.base_offset,
            effective_start,
        );
        let chunk = segment::read_batches(
            fs,
            &snap.active_meta.log_path,
            pos,
            effective_start,
            max_bytes.saturating_sub(out.len()),
        )?;
        out.extend_from_slice(&chunk);
    }

    // Cap to max_bytes (read_batches may overshoot the last batch).
    if out.len() > max_bytes {
        out.truncate(max_bytes);
    }

    Ok(out.freeze())
}

/// What [`open_blocking`] recovers off disk, handed back to
/// [`Partition::open`] to assemble into a [`Partition`].
struct OpenedState {
    epoch: i64,
    hwm: i64,
    log_start: i64,
    closed: Vec<SegmentMeta>,
    active: ActiveSegment,
    producer_states: HashMap<i64, ProducerEntry>,
}

/// Blocking prologue of [`Partition::open`] — runs on a
/// `spawn_blocking` thread (gh #210).
///
/// Every call in here is synchronous `std::io` against the partition
/// directory, and `scan_high_watermark` walks the entire active
/// segment (up to `segment.bytes`, 1 GiB by default) to find the true
/// high-watermark. That walk is inherent — the log is authoritative
/// and the manifest only best-effort — so unlike the gh #209 read
/// path there is no index to shortcut it. What was wrong was *where*
/// it ran: on a scheduler worker, it took brokers off the air at
/// every takeover. With `limits.cpu: 2` tokio sizes its pool to two
/// workers, so one NFS-backed scan starved the runtime long enough to
/// fail the readiness probe — which dropped the broker from the
/// controller's alive set, triggered reassignment, and caused *more*
/// takeovers (gh #208).
/// Number of [`open_blocking`] attempts before an ENOENT is surfaced.
const OPEN_ENOENT_ATTEMPTS: u32 = 5;
/// First backoff between those attempts; doubles each time (50 + 100 +
/// 200 + 400 = 750 ms total).
const OPEN_ENOENT_BACKOFF: std::time::Duration = std::time::Duration::from_millis(50);

/// [`open_blocking`], retried while the directory keeps vanishing under
/// it (gh #220).
///
/// `open_blocking` starts with `mkdir_all`, so an ENOENT after that
/// point never means "this path is wrong" — it means someone removed the
/// tree between our mkdir and our first file open. In practice that
/// someone is the operator reclaiming a previous incarnation of the
/// topic: it renames the live directory aside and re-creates it, and an
/// open landing inside that window fails on a path that is about to
/// exist again.
///
/// Retrying is the right response because the whole prologue is
/// idempotent — it re-creates the directory, re-lists segments, and
/// re-scans. Failing instead pushes the problem onto callers that can
/// only translate it into a produce error or a partition that stays
/// closed until the next takeover reconcile.
fn open_blocking_retrying(fs: &dyn Fs, dir: &std::path::Path) -> Result<OpenedState, StorageError> {
    let mut backoff = OPEN_ENOENT_BACKOFF;
    for _ in 1..OPEN_ENOENT_ATTEMPTS {
        match open_blocking(fs, dir) {
            // Silent by design — this crate carries no logger, and the
            // caller already logs the error if every attempt fails.
            Err(StorageError::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => {
                std::thread::sleep(backoff);
                backoff *= 2;
            }
            other => return other,
        }
    }
    // Last attempt: whatever it returns is the answer.
    open_blocking(fs, dir)
}

fn open_blocking(fs: &dyn Fs, dir: &std::path::Path) -> Result<OpenedState, StorageError> {
    fs.mkdir_all(dir)?;

    // Manifest is the source of truth for (epoch, hwm, log_start)
    // when present; segment scan is the fallback.
    let manifest_read = manifest::read(fs, dir).map_err(|e| match e {
        manifest::ManifestError::Io(e) => StorageError::Io(e),
        manifest::ManifestError::Json(e) => StorageError::Json(e),
        other => StorageError::Io(std::io::Error::other(other.to_string())),
    })?;
    let (epoch, hwm, log_start) = match manifest_read {
        ReadResult::Present(m) => (m.epoch, m.high_watermark, m.log_start_offset),
        ReadResult::Legacy(m) => (m.epoch, 0, 0),
        ReadResult::NotFound => (0, 0, 0),
    };

    // Load any existing segments.
    let mut closed = segment::list_segments(fs, dir)?;

    // The active segment is the last one (highest base_offset);
    // if none exist yet, create at offset 0 with our current
    // epoch.
    let active = if let Some(last) = closed.pop() {
        let mut a = ActiveSegment::open_meta_only(last);
        a.open_handles(fs)?;
        a
    } else {
        ActiveSegment::create(fs, dir, hwm, epoch)?
    };

    // Recover the high-watermark by scanning the active segment
    // forward, stopping at the first torn batch. The log is
    // authoritative; the manifest HWM is best-effort and the scan
    // result overrides it (advancing when the manifest lagged, or
    // rewinding a phantom HWM the manifest claimed but the log
    // doesn't back).
    //
    // gh #recovery-checkpoint: if a durable checkpoint refers to *this*
    // active segment and its byte position is within the log, resume
    // the scan from there — everything before it was fsynced and is
    // trusted, so a graceful restart (checkpoint at EOF) reads nothing.
    // Otherwise — no checkpoint, or a roll happened since it was
    // written, or the log was truncated below it — fall back to a full
    // scan of the active segment, which is always correct and, with a
    // bounded segment size, cheap.
    let (scan_start, scan_seed) = match recovery_checkpoint::read(fs, dir) {
        Some(cp)
            if cp.segment_base == active.meta.base_offset
                && cp.byte_pos >= 0
                && u64::try_from(cp.byte_pos).unwrap_or(u64::MAX) <= active.log_size() =>
        {
            (u64::try_from(cp.byte_pos).unwrap_or(0), cp.high_watermark)
        }
        _ => (0, active.meta.base_offset),
    };
    let hwm = {
        let mut f = fs.open_read(&active.meta.log_path)?;
        segment::scan_high_watermark_from(&mut f, scan_start, scan_seed)?
    };

    // Restore the idempotence window.
    let producer_states = read_producer_snapshot(fs, dir)
        .map_err(|e| match e {
            crate::producer_snapshot::ProducerSnapshotError::Io(e) => StorageError::Io(e),
            crate::producer_snapshot::ProducerSnapshotError::Json(e) => StorageError::Json(e),
        })?
        .map(|entries| entries.into_iter().collect::<HashMap<i64, ProducerEntry>>())
        .unwrap_or_default();

    // Persist the manifest so the next open is fast.
    manifest::write(
        fs,
        dir,
        &Manifest {
            epoch,
            high_watermark: hwm,
            log_start_offset: log_start,
        },
    )
    .map_err(|e| match e {
        manifest::ManifestError::Io(e) => StorageError::Io(e),
        manifest::ManifestError::Json(e) => StorageError::Json(e),
        other => StorageError::Io(std::io::Error::other(other.to_string())),
    })?;

    Ok(OpenedState {
        epoch,
        hwm,
        log_start,
        closed,
        active,
        producer_states,
    })
}

struct PartitionInner {
    dir: PathBuf,
    active: ActiveSegment,
    closed: Vec<SegmentMeta>,
    log_start: i64,
    high_water: i64,
    epoch: i64,

    /// Records appended since the last flush request was sent. Reset
    /// on send (not on completion) so we don't enqueue redundant
    /// flushes for the same batch tail.
    pending_flush_records: i64,
    /// gh #recovery-checkpoint: byte position of the active segment
    /// covered by the most recently written recovery checkpoint. The
    /// committer writes a new checkpoint once the fsynced log has grown
    /// [`CHECKPOINT_INTERVAL_BYTES`] past this, bounding how much of the
    /// tail a crash has to re-scan.
    last_checkpoint_byte: i64,
    /// Monotonic counter incremented every time append decides to
    /// trigger a flush. Each appender's local copy of this value is
    /// what `acks=all` waits on `completed_flush_seq` to cover.
    requested_flush_seq: u64,
    /// Updated by the committer task after each successful fsync.
    completed_flush_seq: u64,
    /// Sticky on first error. Subsequent appends return immediately
    /// with this error; the broker is expected to drop the partition.
    flush_err: Option<StorageError>,

    /// Per-PID idempotence window (gh #12). Held directly here (not
    /// behind [`crate::idempotence::ProducerStates`]) so the
    /// classify+record_accepted pair runs under the partition mutex
    /// without a nested lock.
    producer_states: HashMap<i64, ProducerEntry>,

    /// gh #176 — first offset of each currently-open transactional
    /// producer on this partition. `min(values)` is the Last Stable
    /// Offset (LSO); `read_committed` Fetch reads only up to LSO.
    /// Updated at append time alongside `producer_states`.
    open_txns: OpenTxnIndex,
    /// gh #176 — completed-but-aborted transactions whose ABORT
    /// marker is still live in the log. Drives the Fetch response's
    /// `AbortedTransactions[]` list. Evicted as `log_start` advances.
    aborted_txns: AbortedTxnIndex,
}

struct FlushCoord {
    req_tx: mpsc::Sender<()>,
    /// Appenders waiting on `acks=-1` park here; the committer
    /// notifies after each cycle (success or failure).
    cond: Arc<Notify>,
}

/// One owned, leader-bound partition. Holds the active segment's
/// file handles per the gh #76 single-FD contract.
pub struct Partition {
    inner: Arc<Mutex<PartitionInner>>,
    snapshot: ArcSwap<ReadSnapshot>,
    flush: FlushCoord,
    committer: Mutex<Option<JoinHandle<()>>>,
    cfg: PartitionConfig,
    fs: Arc<dyn Fs>,
    topic: String,
    partition: i32,
}

impl std::fmt::Debug for Partition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Partition")
            .field("topic", &self.topic)
            .field("partition", &self.partition)
            .field("dir", &self.inner.lock().dir)
            .finish()
    }
}

impl Partition {
    /// Open (or create) a partition at `dir` and immediately take leadership:
    /// open file handles, restore producer state, start the committer task,
    /// publish the initial [`ReadSnapshot`]. After [`Partition::open`] returns,
    /// the partition is ready to accept appends.
    pub async fn open(
        fs: Arc<dyn Fs>,
        topic: String,
        partition: i32,
        dir: PathBuf,
        cfg: PartitionConfig,
    ) -> Result<Self, StorageError> {
        // gh #210: the whole recovery prologue is synchronous I/O and
        // the high-watermark scan walks up to a full segment, so it
        // runs on a blocking thread. Everything after it is cheap and
        // needs the runtime (the committer task spawn).
        let fs_open = fs.clone();
        let dir_open = dir.clone();
        let OpenedState {
            epoch,
            hwm,
            log_start,
            closed,
            active,
            producer_states,
        } = tokio::task::spawn_blocking(move || {
            open_blocking_retrying(fs_open.as_ref(), &dir_open)
        })
        .await
        .map_err(|e| StorageError::Io(std::io::Error::other(e)))??;

        let initial_snapshot = ReadSnapshot {
            high_water: hwm,
            log_start,
            epoch,
            closed: Arc::new(closed.clone()),
            active_meta: active.meta.clone(),
        };

        let inner = Arc::new(Mutex::new(PartitionInner {
            dir: dir.clone(),
            active,
            closed,
            log_start,
            high_water: hwm,
            epoch,
            pending_flush_records: 0,
            last_checkpoint_byte: 0,
            requested_flush_seq: 0,
            completed_flush_seq: 0,
            flush_err: None,
            producer_states,
            open_txns: OpenTxnIndex::new(),
            aborted_txns: AbortedTxnIndex::new(),
        }));

        // Flush channel — capacity 1 with coalescing semantics. If
        // multiple appenders signal a flush before the committer
        // wakes, only one wake-up happens; the committer's snapshot
        // of `requested_flush_seq` covers all of them.
        let (req_tx, req_rx) = mpsc::channel::<()>(1);
        let cond = Arc::new(Notify::new());

        let committer_handle = spawn_committer(
            inner.clone(),
            cond.clone(),
            req_rx,
            cfg.fsync_max_latency,
            fs.clone(),
        );

        Ok(Self {
            inner,
            snapshot: ArcSwap::from(Arc::new(initial_snapshot)),
            flush: FlushCoord { req_tx, cond },
            committer: Mutex::new(Some(committer_handle)),
            cfg,
            fs,
            topic,
            partition,
        })
    }

    /// Drain the committer, persist manifest + producer snapshot one
    /// last time, close the active segment's handles. After close
    /// returns, the partition's FDs are released and a new
    /// [`Partition::open`] on the same `dir` will pick up the
    /// persisted state.
    pub async fn close(&self) -> Result<(), StorageError> {
        // Stop accepting flush requests by dropping the sender ↔
        // we can't drop self.flush.req_tx without &mut self. Instead
        // we'll close the channel by dropping all outstanding senders,
        // which requires a different approach — for now, the
        // committer is also signalled via mpsc::Sender::closed() when
        // we explicitly drop the Sender. Use a sentinel: take() the
        // JoinHandle so future closes are no-ops.
        let handle = {
            let mut guard = self.committer.lock();
            guard.take()
        };
        let Some(handle) = handle else {
            return Ok(()); // already closed
        };

        // Persist state under the lock, then close handles.
        {
            let mut guard = self.inner.lock();
            persist_state_locked(self.fs.as_ref(), &mut guard)?;
            // gh #recovery-checkpoint: sync the log tail and checkpoint
            // the whole active segment as durable, so the next open
            // (this broker or the next leader, off the shared volume)
            // re-scans nothing — the graceful-restart fast path. Only
            // on a successful sync; if the log can't be flushed, skip
            // the checkpoint and let the next open fall back to a scan.
            let checkpoint = if guard.active.sync_log().is_ok() {
                let durable = i64::try_from(guard.active.log_size()).unwrap_or(i64::MAX);
                guard.last_checkpoint_byte = durable;
                Some(RecoveryCheckpoint {
                    segment_base: guard.active.meta.base_offset,
                    byte_pos: durable,
                    high_watermark: guard.high_water,
                })
            } else {
                None
            };
            let dir = guard.dir.clone();
            guard.active.close_handles();
            drop(guard);
            if let Some(cp) = checkpoint {
                let _ = recovery_checkpoint::write(self.fs.as_ref(), &dir, &cp);
            }
        }

        // Drop the channel sender via Arc/internal swap... can't,
        // since FlushCoord doesn't expose mutability. Instead we
        // signal the committer to exit by aborting the JoinHandle —
        // the committer holds no resources that need orderly cleanup
        // beyond the mutex it briefly takes.
        handle.abort();
        let _ = handle.await;
        Ok(())
    }

    /// Drop this partition **without persisting anything** (gh #219).
    ///
    /// The counterpart to [`Partition::close`], for when the topic
    /// itself is gone: stop the committer and release the FDs, but
    /// write no manifest, no producer snapshot, no recovery checkpoint.
    ///
    /// Persisting here is actively harmful. The operator reclaims a
    /// deleted topic's directory by renaming it aside, and a recreated
    /// topic gets a *fresh* directory at the same path — so a
    /// well-meaning `close()` racing that sequence lands the dead
    /// incarnation's high watermark and idempotence window in the new
    /// incarnation's directory. That is the "empty log, advanced
    /// HWM == logStartOffset" state gh #219 saw on the Streams
    /// repartition topic.
    ///
    /// Releasing the FDs still matters: on NFS an unlinked-while-open
    /// file becomes a `.nfsXXXX` entry that blocks the parent's removal
    /// (gh #76), so dropping handles promptly is what lets the staged
    /// directory actually be reclaimed.
    pub async fn abandon(&self) {
        let handle = {
            let mut guard = self.committer.lock();
            guard.take()
        };
        {
            let mut guard = self.inner.lock();
            guard.active.close_handles();
        }
        if let Some(handle) = handle {
            handle.abort();
            let _ = handle.await;
        }
    }

    /// Append a raw RecordBatch under `epoch`. Returns the assigned
    /// `base_offset` (or the cached one for an idempotent duplicate).
    pub async fn append(&self, epoch: u32, acks: i16, batch: Bytes) -> Result<i64, StorageError> {
        // Pre-lock parse so we don't hold the mutex during this work.
        let prod_info: Option<BatchProducerInfo> = if batch.len() >= 57 {
            parse_batch_producer_info(&batch).ok()
        } else {
            None
        };
        let (assigned_base, my_flush_seq, triggered_flush) = {
            let mut guard = self.inner.lock();

            // Early-fail if the partition is in a sticky stall.
            if guard.flush_err.is_some() {
                // The original cause is captured in `flush_err` for
                // diagnostics; we surface `Stalled` to the wire so
                // every dead-partition response is uniform.
                return Err(StorageError::Stalled);
            }

            // Epoch fence — caller's epoch must be ≥ our current
            // epoch; a stale epoch is rejected.
            if i64::from(epoch) < guard.epoch {
                return Err(StorageError::EpochMismatch);
            }

            // Idempotence: classify before touching the log.
            if let Some(info) = prod_info {
                match idempotence::classify(&guard.producer_states, info) {
                    Outcome::Duplicate { base_offset } => return Ok(base_offset),
                    Outcome::OutOfOrder => return Err(StorageError::OutOfOrderSequence),
                    Outcome::InvalidEpoch => return Err(StorageError::InvalidProducerEpoch),
                    Outcome::Accept | Outcome::NotIdempotent => {}
                }
            }

            // Roll if the next append would exceed segment_bytes.
            let batch_len_u64 = u64::try_from(batch.len()).unwrap_or(u64::MAX);
            let projected_size = guard.active.log_size().saturating_add(batch_len_u64);
            if projected_size > self.cfg.segment_bytes && guard.active.log_size() > 0 {
                let new_base = guard.high_water;
                let new_epoch = guard.epoch;
                let dir = guard.dir.clone();
                // Move the old active out so roll_fast can consume it.
                let old_active = std::mem::replace(
                    &mut guard.active,
                    // Placeholder — immediately overwritten below.
                    ActiveSegment::open_meta_only(SegmentMeta {
                        base_offset: 0,
                        epoch: 0,
                        size: 0,
                        log_path: dir.join("_placeholder.log"),
                        index_path: dir.join("_placeholder.index"),
                    }),
                );
                let (new_active, tail) =
                    old_active.roll_fast(self.fs.as_ref(), &dir, new_base, new_epoch)?;
                guard.closed.push(tail.closed_meta);
                guard.active = new_active;
                // Deferred index-fsync + close runs off-lock on the
                // blocking pool.
                tokio::task::spawn_blocking(move || {
                    let _ = (tail.finalize)();
                });
            }

            // Rewrite baseOffset → current HWM. v2 CRC covers byte
            // 21 onward, so this overwrite is wire-correct.
            let assigned = guard.high_water;
            let mut owned = bytes::BytesMut::with_capacity(batch.len());
            owned.extend_from_slice(&batch);
            owned[0..8].copy_from_slice(&assigned.to_be_bytes());

            // Pre-parse the offset delta so we know how much HWM advances.
            let (_base, last_offset_delta, _max_ts) =
                parse_batch_offsets(&owned).map_err(StorageError::Io)?;

            // Append the bytes to the active log.
            guard
                .active
                .append_batch(&owned, self.cfg.index_interval_bytes)
                .map_err(StorageError::Io)?;

            // Advance accounting.
            guard.high_water = assigned + i64::from(last_offset_delta) + 1;
            let advanced_records = i64::from(last_offset_delta) + 1;
            guard.pending_flush_records += advanced_records;

            // Record the idempotence outcome only after the log write
            // succeeded (so a failed append doesn't poison the window).
            if let Some(info) = prod_info {
                idempotence::record_accepted(&mut guard.producer_states, info, assigned);
            }

            // gh #176 — update the open + aborted-txn indexes off the
            // same batch. Same byte-opacity contract: we only read the
            // header attrs + pid + (for control batches) the key's
            // type byte.
            if let Some(txn_info) = parse_batch_txn_info(&owned) {
                if txn_info.is_transactional {
                    if txn_info.is_control {
                        // COMMIT or ABORT marker — close the open txn.
                        if let Some(first_offset) = guard.open_txns.close(txn_info.producer_id) {
                            if matches!(txn_info.control_commit, Some(false)) {
                                guard.aborted_txns.record(AbortedTxn {
                                    producer_id: txn_info.producer_id,
                                    first_offset,
                                    last_offset: assigned,
                                });
                            }
                        }
                    } else {
                        // Transactional data batch — record the first
                        // offset for this pid's current txn (no-op if
                        // already recorded).
                        guard
                            .open_txns
                            .record_data_batch(txn_info.producer_id, assigned);
                    }
                }
            }

            // Decide whether to fire a flush request.
            let trigger = self.cfg.flush_interval_messages > 0
                && guard.pending_flush_records >= self.cfg.flush_interval_messages;
            let my_seq;
            if trigger {
                guard.requested_flush_seq += 1;
                my_seq = guard.requested_flush_seq;
                guard.pending_flush_records = 0;
            } else {
                my_seq = guard.requested_flush_seq;
            }

            // Republish the read snapshot.
            self.snapshot.store(Arc::new(ReadSnapshot {
                high_water: guard.high_water,
                log_start: guard.log_start,
                epoch: guard.epoch,
                closed: Arc::new(guard.closed.clone()),
                active_meta: guard.active.meta.clone(),
            }));

            (assigned, my_seq, trigger)
        };

        if triggered_flush {
            // Coalesced: try_send fails fast if a flush is already
            // queued (capacity-1 channel). The committer picks up the
            // latest `requested_flush_seq` under the lock anyway.
            let _ = self.flush.req_tx.try_send(());
        }

        // acks == -1: wait for the fsync — but ONLY when this append
        // is the one that crossed the flush-interval threshold. This
        // mirrors the v0.1 engine's `waitForFlushIfAcksAllLocked`
        // (`triggeredFlushSeq <= 0 → return nil`): with
        // `flush_interval_messages == 1` every append triggers, so
        // every acks=all append waits — honest semantics. With the
        // interval raised (the durability/throughput dial), only the
        // crossing append pays the fsync latency; everything else
        // acks immediately, including appends landing while a flush
        // is in flight. The previous shape waited on the
        // last-requested seq from EVERY acks=all append, parking the
        // whole pipeline for the full fsync window each cycle — worth
        // −26% Strimzi-relative throughput vs the v0.1 flavor at
        // interval 10000 on NFS (phase 9 A.3 gate, gh #188).
        if acks == -1 && triggered_flush {
            self.await_flush(my_flush_seq).await?;
        }

        Ok(assigned_base)
    }

    /// Park until the committer has fsynced past `target_seq`, or the
    /// partition enters a sticky stall.
    async fn await_flush(&self, target_seq: u64) -> Result<(), StorageError> {
        loop {
            let notified = self.flush.cond.notified();
            // Check before parking — committer may have already
            // satisfied target_seq before our subscribe.
            {
                let guard = self.inner.lock();
                if guard.flush_err.is_some() {
                    return Err(StorageError::Stalled);
                }
                if guard.completed_flush_seq >= target_seq {
                    return Ok(());
                }
            }
            notified.await;
        }
    }

    /// Read raw RecordBatch bytes from `start_offset` up to
    /// `max_bytes`. Walks closed segments then the active segment;
    /// stops at the first hit that fills the cap.
    pub async fn read(&self, start_offset: i64, max_bytes: usize) -> Result<Bytes, StorageError> {
        // Capture the segment list under the snapshot (lock-free) so
        // a concurrent roll doesn't move the active segment under us.
        let snap = self.snapshot.load_full();
        let fs = self.fs.clone();
        // gh #209: segment reads are synchronous `std::io` against a
        // (typically NFS) log file, with unbounded latency. Running
        // them inline on a scheduler worker starved the whole runtime
        // — with `limits.cpu: 2` tokio sizes the pool to two workers,
        // so a single large fetch took the process off the air:
        // no accepts, no /readyz, no background tasks.
        tokio::task::spawn_blocking(move || {
            read_snapshot(fs.as_ref(), &snap, start_offset, max_bytes)
        })
        .await
        .map_err(|e| StorageError::Io(std::io::Error::other(e)))?
    }

    /// Advance the log-start offset to (at least) `target_offset`.
    /// Drops closed segments fully below the new log_start.
    /// `target_offset == -1` means "purge to HWM" (KIP-107).
    pub async fn delete_records(&self, target_offset: i64) -> Result<i64, StorageError> {
        let new_log_start = {
            let mut guard = self.inner.lock();
            let new_start = if target_offset < 0 {
                guard.high_water
            } else if target_offset > guard.high_water {
                return Err(StorageError::OffsetOutOfRange);
            } else {
                target_offset
            };
            if new_start > guard.log_start {
                guard.log_start = new_start;

                // Drop closed segments fully below the new log_start.
                // Conservatively: drop any closed seg whose
                // base_offset of the NEXT seg is <= log_start.
                // For Phase 2 simplicity we only drop a seg if its
                // size is 0 OR the next seg starts past log_start.
                // Use the next seg's base_offset as the prev seg's
                // exclusive upper bound; the last closed seg's upper
                // bound is the active seg's base_offset.
                let active_base = guard.active.meta.base_offset;
                let mut new_closed: Vec<SegmentMeta> = Vec::new();
                for (i, seg) in guard.closed.iter().enumerate() {
                    let upper = guard
                        .closed
                        .get(i + 1)
                        .map(|n| n.base_offset)
                        .unwrap_or(active_base);
                    if upper <= new_start {
                        // Entire segment below log_start — remove the
                        // file from disk (best-effort).
                        let _ = self.fs.remove(&seg.log_path);
                        let _ = self.fs.remove(&seg.index_path);
                    } else {
                        new_closed.push(seg.clone());
                    }
                }
                guard.closed = new_closed;

                self.snapshot.store(Arc::new(ReadSnapshot {
                    high_water: guard.high_water,
                    log_start: guard.log_start,
                    epoch: guard.epoch,
                    closed: Arc::new(guard.closed.clone()),
                    active_meta: guard.active.meta.clone(),
                }));
            }
            guard.log_start
        };
        Ok(new_log_start)
    }

    /// Lock-free HWM read via the published snapshot.
    pub fn high_watermark(&self) -> i64 {
        self.snapshot.load().high_water
    }

    /// Lock-free log_start read via the published snapshot.
    pub fn log_start_offset(&self) -> i64 {
        self.snapshot.load().log_start
    }

    /// Lock-free epoch read via the published snapshot.
    pub fn epoch(&self) -> i64 {
        self.snapshot.load().epoch
    }

    /// gh #176 — Last Stable Offset for `read_committed` Fetch.
    /// The lowest offset across all currently-open transactional
    /// producers on this partition, or HWM when no txn is open.
    /// Mirrors Apache's `Log.lastStableOffset`.
    pub fn last_stable_offset(&self) -> i64 {
        let guard = self.inner.lock();
        guard
            .open_txns
            .min_open_offset()
            .unwrap_or(guard.high_water)
    }

    /// gh #176 — aborted transactions whose first-offset falls in
    /// `[start_offset, end_offset)`. Used to populate the Fetch
    /// response's `AbortedTransactions[]` list for `read_committed`
    /// consumers.
    pub fn aborted_in_range(&self, start_offset: i64, end_offset: i64) -> Vec<AbortedTxn> {
        let guard = self.inner.lock();
        guard.aborted_txns.in_range(start_offset, end_offset)
    }

    /// gh #30 / #108: bump the recorded producer epoch and clear
    /// this partition's dedupe window for `pid`. Idempotent — no-op
    /// when the recorded epoch is already `>= new_epoch`. Called by
    /// the storage engine's cross-partition `fence_producer_epoch`
    /// walker after an `InitProducerId` epoch bump, both for the
    /// local broker's in-process fence and for inbound peer fences
    /// the `kaas-broker::FenceWatcher` dispatches.
    pub fn fence_producer(&self, pid: i64, new_epoch: i16) {
        let mut guard = self.inner.lock();
        let entry = guard.producer_states.entry(pid).or_default();
        if new_epoch > entry.epoch {
            entry.epoch = new_epoch;
            entry.recent.clear();
        }
    }

    /// Sum of closed-segment sizes + active-segment size.
    pub fn partition_size(&self) -> i64 {
        let snap = self.snapshot.load();
        let total: u64 = snap
            .closed
            .iter()
            .map(|s| s.size)
            .chain(std::iter::once(snap.active_meta.size))
            .sum();
        i64::try_from(total).unwrap_or(i64::MAX)
    }

    /// Size-based retention: return the `target_offset` that
    /// [`Partition::delete_records`] should advance to in order to
    /// keep the partition under `retention_bytes`. Returns `None`
    /// when no cleanup is needed.
    ///
    /// Lock-free — reads via the [`ReadSnapshot`] only. Closed
    /// segments are walked oldest-first; segments are virtually
    /// dropped while `cumulative_size <= surplus`. The returned
    /// offset is the `base_offset` of the first segment we KEEP,
    /// or the active segment's `base_offset` if all closed segments
    /// would be dropped (the active segment is never reclaimed —
    /// neither here nor by `DeleteRecords`, which also unlinks
    /// closed segments only).
    pub fn cleanup_target_for_size_bytes(&self, retention_bytes: u64) -> Option<i64> {
        let snap = self.snapshot.load();
        let active_size = snap.active_meta.size;
        let active_base = snap.active_meta.base_offset;
        let closed_sizes: Vec<u64> = snap.closed.iter().map(|s| s.size).collect();
        let total: u64 = closed_sizes.iter().sum::<u64>().saturating_add(active_size);
        if total <= retention_bytes {
            return None;
        }
        let mut surplus = total - retention_bytes;
        // Walk oldest-first.
        for (i, seg) in snap.closed.iter().enumerate() {
            if seg.size > surplus {
                // Dropping this segment would over-shoot retention.
                // Stop here — keep this segment, return its base.
                return Some(seg.base_offset);
            }
            surplus -= seg.size;
            if surplus == 0 {
                // Exactly at retention after dropping up to and
                // including segment `i`. Target = next segment's
                // base (or active if this was the last closed).
                return Some(
                    snap.closed
                        .get(i + 1)
                        .map(|n| n.base_offset)
                        .unwrap_or(active_base),
                );
            }
        }
        // Dropping all closed segments still leaves a surplus
        // (active alone exceeds retention). Cap at active_base —
        // the active segment is not reclaimed by the cleaner.
        Some(active_base)
    }
}

// ---------------------------------------------------------------------------
// Persist helper — called under the inner lock.
// ---------------------------------------------------------------------------

fn persist_state_locked(fs: &dyn Fs, guard: &mut PartitionInner) -> Result<(), StorageError> {
    let snap: Vec<(i64, ProducerEntry)> = guard
        .producer_states
        .iter()
        .map(|(k, v)| (*k, v.clone()))
        .collect();
    write_producer_snapshot(fs, &guard.dir, &snap).map_err(|e| match e {
        crate::producer_snapshot::ProducerSnapshotError::Io(e) => StorageError::Io(e),
        crate::producer_snapshot::ProducerSnapshotError::Json(e) => StorageError::Json(e),
    })?;
    manifest::write(
        fs,
        &guard.dir,
        &Manifest {
            epoch: guard.epoch,
            high_watermark: guard.high_water,
            log_start_offset: guard.log_start,
        },
    )
    .map_err(|e| match e {
        manifest::ManifestError::Io(e) => StorageError::Io(e),
        manifest::ManifestError::Json(e) => StorageError::Json(e),
        other => StorageError::Io(std::io::Error::other(other.to_string())),
    })?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Committer task
// ---------------------------------------------------------------------------

fn spawn_committer(
    inner: Arc<Mutex<PartitionInner>>,
    cond: Arc<Notify>,
    mut req_rx: mpsc::Receiver<()>,
    fsync_max_latency: Duration,
    fs: Arc<dyn Fs>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        while req_rx.recv().await.is_some() {
            // Capture the seq we want to satisfy.
            let target_seq = inner.lock().requested_flush_seq;
            if target_seq == inner.lock().completed_flush_seq {
                // Already satisfied by a previous cycle that
                // coalesced multiple requests.
                continue;
            }

            let inner_clone = inner.clone();
            let fs_c = fs.clone();
            let fsync_handle = tokio::task::spawn_blocking(move || -> std::io::Result<u64> {
                // Sync under the lock, then decide whether a fresh
                // recovery checkpoint is due — but write it OFF the lock
                // so the tmp+rename doesn't stall appenders.
                let (seq, checkpoint) = {
                    let mut guard = inner_clone.lock();
                    guard.active.sync_log()?;
                    let seq = guard.requested_flush_seq;
                    let durable = i64::try_from(guard.active.log_size()).unwrap_or(i64::MAX);
                    let cp = if durable.saturating_sub(guard.last_checkpoint_byte)
                        >= CHECKPOINT_INTERVAL_BYTES
                    {
                        // Advance optimistically: a failed write just
                        // defers the next attempt one interval, and
                        // recovery falls back to a full scan regardless.
                        guard.last_checkpoint_byte = durable;
                        Some((
                            RecoveryCheckpoint {
                                segment_base: guard.active.meta.base_offset,
                                byte_pos: durable,
                                high_watermark: guard.high_water,
                            },
                            guard.dir.clone(),
                        ))
                    } else {
                        None
                    };
                    (seq, cp)
                };
                if let Some((cp, dir)) = checkpoint {
                    let _ = recovery_checkpoint::write(fs_c.as_ref(), &dir, &cp);
                }
                Ok(seq)
            });

            let outcome = tokio::time::timeout(fsync_max_latency, fsync_handle).await;
            match outcome {
                Ok(Ok(Ok(satisfied_seq))) => {
                    let mut guard = inner.lock();
                    if satisfied_seq > guard.completed_flush_seq {
                        guard.completed_flush_seq = satisfied_seq;
                    }
                    drop(guard);
                    cond.notify_waiters();
                }
                Ok(Ok(Err(io_err))) => {
                    let mut guard = inner.lock();
                    if guard.flush_err.is_none() {
                        guard.flush_err = Some(StorageError::Io(io_err));
                    }
                    drop(guard);
                    cond.notify_waiters();
                }
                Ok(Err(_join_err)) => {
                    // spawn_blocking panicked — treat as Stalled.
                    let mut guard = inner.lock();
                    if guard.flush_err.is_none() {
                        guard.flush_err = Some(StorageError::Stalled);
                    }
                    drop(guard);
                    cond.notify_waiters();
                }
                Err(_elapsed) => {
                    // Timeout (gh #95). The orphaned spawn_blocking
                    // task is still running; that's fine — it drains
                    // when the kernel eventually returns. We set
                    // sticky Stalled and move on.
                    let mut guard = inner.lock();
                    if guard.flush_err.is_none() {
                        guard.flush_err = Some(StorageError::Stalled);
                    }
                    drop(guard);
                    cond.notify_waiters();
                }
            }
        }
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::RealFs;

    /// [`RealFs`] with a deliberate stall on every `open_read`, to
    /// stand in for NFS latency.
    struct SlowFs {
        inner: RealFs,
        delay: Duration,
    }

    /// [`RealFs`] whose first `n` `create` calls fail with ENOENT —
    /// standing in for the operator renaming the topic dir aside
    /// between our `mkdir_all` and our first file open (gh #220).
    struct VanishingFs {
        inner: RealFs,
        fail_creates: std::sync::atomic::AtomicU32,
    }

    impl Fs for VanishingFs {
        fn open_read(&self, p: &std::path::Path) -> std::io::Result<Box<dyn crate::fs::FileRead>> {
            self.inner.open_read(p)
        }
        fn open_write(
            &self,
            p: &std::path::Path,
            append: bool,
        ) -> std::io::Result<Box<dyn crate::fs::FileWrite>> {
            self.inner.open_write(p, append)
        }
        fn create(&self, p: &std::path::Path) -> std::io::Result<Box<dyn crate::fs::FileWrite>> {
            if self
                .fail_creates
                .fetch_update(
                    std::sync::atomic::Ordering::SeqCst,
                    std::sync::atomic::Ordering::SeqCst,
                    |n| n.checked_sub(1),
                )
                .is_ok()
            {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "no such file or directory",
                ));
            }
            self.inner.create(p)
        }
        fn fsync(&self, f: &mut dyn crate::fs::FileWrite) -> std::io::Result<()> {
            self.inner.fsync(f)
        }
        fn rename(&self, from: &std::path::Path, to: &std::path::Path) -> std::io::Result<()> {
            self.inner.rename(from, to)
        }
        fn remove(&self, p: &std::path::Path) -> std::io::Result<()> {
            self.inner.remove(p)
        }
        fn mkdir_all(&self, p: &std::path::Path) -> std::io::Result<()> {
            self.inner.mkdir_all(p)
        }
        fn readdir(&self, p: &std::path::Path) -> std::io::Result<Vec<PathBuf>> {
            self.inner.readdir(p)
        }
        fn stat(&self, p: &std::path::Path) -> std::io::Result<std::fs::Metadata> {
            self.inner.stat(p)
        }
    }

    impl Fs for SlowFs {
        fn open_read(&self, p: &std::path::Path) -> std::io::Result<Box<dyn crate::fs::FileRead>> {
            std::thread::sleep(self.delay);
            self.inner.open_read(p)
        }
        fn open_write(
            &self,
            p: &std::path::Path,
            append: bool,
        ) -> std::io::Result<Box<dyn crate::fs::FileWrite>> {
            self.inner.open_write(p, append)
        }
        fn create(&self, p: &std::path::Path) -> std::io::Result<Box<dyn crate::fs::FileWrite>> {
            self.inner.create(p)
        }
        fn fsync(&self, f: &mut dyn crate::fs::FileWrite) -> std::io::Result<()> {
            self.inner.fsync(f)
        }
        fn rename(&self, from: &std::path::Path, to: &std::path::Path) -> std::io::Result<()> {
            self.inner.rename(from, to)
        }
        fn remove(&self, p: &std::path::Path) -> std::io::Result<()> {
            self.inner.remove(p)
        }
        fn mkdir_all(&self, p: &std::path::Path) -> std::io::Result<()> {
            self.inner.mkdir_all(p)
        }
        fn readdir(&self, p: &std::path::Path) -> std::io::Result<Vec<PathBuf>> {
            self.inner.readdir(p)
        }
        fn stat(&self, p: &std::path::Path) -> std::io::Result<std::fs::Metadata> {
            self.inner.stat(p)
        }
    }

    /// gh #210: `Partition::open` recovers by walking the active
    /// segment, which on NFS costs seconds. It used to do that inline
    /// on a scheduler worker, and with `limits.cpu: 2` tokio only has
    /// two — so a takeover took the broker off the air entirely: no
    /// accepts, no `/readyz`, no background tasks.
    ///
    /// Both futures are `spawn`ed so they contend for the *same*
    /// single worker. Driving `open` on `block_on`'s calling thread
    /// instead would let the ticker run on the worker regardless, and
    /// the test would pass even with the blocking version.
    #[test]
    fn open_does_not_starve_the_runtime() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("t").join("0");

        let rt1 = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();

        let ticks = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let ticker_ticks = ticks.clone();

        let p = rt1.block_on(async move {
            let ticker = tokio::spawn(async move {
                loop {
                    ticker_ticks.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    tokio::task::yield_now().await;
                }
            });
            let fs: Arc<dyn Fs> = Arc::new(SlowFs {
                inner: RealFs::new(),
                delay: Duration::from_millis(40),
            });
            let opened = tokio::spawn(async move {
                Partition::open(fs, "t".to_owned(), 0, dir, PartitionConfig::default()).await
            });
            let p = opened.await.unwrap().unwrap();
            ticker.abort();
            p
        });

        let observed = ticks.load(std::sync::atomic::Ordering::Relaxed);
        assert!(
            observed > 50,
            "runtime advanced only {observed} ticks while Partition::open ran — \
             the recovery scan is blocking the scheduler worker"
        );
        drop(p);
    }

    fn rt() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap()
    }

    /// Build a v2 batch with given (base_offset, num_records). The
    /// engine rewrites bytes [0..8] to its assigned offset.
    fn build_batch(num_records: i32, max_timestamp: i64) -> Bytes {
        let body_size = 49 + 16;
        let total = 12 + body_size;
        let mut buf = vec![0u8; total];
        buf[0..8].copy_from_slice(&0i64.to_be_bytes());
        let body_len_i32 = i32::try_from(body_size).unwrap();
        buf[8..12].copy_from_slice(&body_len_i32.to_be_bytes());
        buf[16] = 2; // magic
        let last_offset_delta = num_records - 1;
        buf[23..27].copy_from_slice(&last_offset_delta.to_be_bytes());
        buf[35..43].copy_from_slice(&max_timestamp.to_be_bytes());
        // Producer ID = -1 (non-idempotent) so no classifier path.
        buf[43..51].copy_from_slice(&(-1i64).to_be_bytes());
        Bytes::from(buf)
    }

    /// Build a transactional v2 data batch (attributes bit 4 set).
    /// PID is encoded; base_sequence is left at 0 so the
    /// idempotence classifier accepts as a fresh first-batch.
    fn build_txn_data_batch(pid: i64, num_records: i32) -> Bytes {
        let body_size = 49 + 16;
        let total = 12 + body_size;
        let mut buf = vec![0u8; total];
        buf[0..8].copy_from_slice(&0i64.to_be_bytes());
        let body_len_i32 = i32::try_from(body_size).unwrap();
        buf[8..12].copy_from_slice(&body_len_i32.to_be_bytes());
        buf[16] = 2;
        // attributes: bit 4 (transactional) only.
        buf[21..23].copy_from_slice(&0x0010i16.to_be_bytes());
        let last_offset_delta = num_records - 1;
        buf[23..27].copy_from_slice(&last_offset_delta.to_be_bytes());
        buf[43..51].copy_from_slice(&pid.to_be_bytes());
        // baseSequence at [53..57] stays 0 = first batch of a fresh
        // PID per the idempotence classifier.
        Bytes::from(buf)
    }

    /// Build a control batch (COMMIT or ABORT marker). attributes
    /// has bits 4 (transactional) | 5 (control) set; base_sequence
    /// = -1 so the idempotence classifier returns NotIdempotent
    /// (markers don't consume sequence slots).
    fn build_marker_batch(pid: i64, commit: bool) -> Bytes {
        let mut buf = vec![0u8; 70];
        buf[0..8].copy_from_slice(&0i64.to_be_bytes());
        // body_size = 58 (everything from attrs through the inline
        // marker record); arbitrary for these tests since we never
        // re-read records from the log.
        buf[8..12].copy_from_slice(&58i32.to_be_bytes());
        buf[16] = 2;
        buf[21..23].copy_from_slice(&0x0030i16.to_be_bytes()); // transactional | control
        buf[43..51].copy_from_slice(&pid.to_be_bytes());
        buf[53..57].copy_from_slice(&(-1i32).to_be_bytes());
        // key type at byte 69: 0=ABORT, 1=COMMIT
        buf[69] = if commit { 1 } else { 0 };
        Bytes::from(buf)
    }

    #[test]
    fn lso_equals_hwm_when_no_txn_open() {
        let tmp = tempfile::tempdir().unwrap();
        rt().block_on(async {
            let fs: Arc<dyn Fs> = Arc::new(RealFs::new());
            let p = Partition::open(
                fs,
                "t".into(),
                0,
                tmp.path().to_path_buf(),
                PartitionConfig::default(),
            )
            .await
            .unwrap();
            p.append(0, -1, build_batch(3, 1_000)).await.unwrap();
            assert_eq!(p.last_stable_offset(), p.high_watermark());
            assert!(p.aborted_in_range(0, i64::MAX).is_empty());
            p.close().await.unwrap();
        });
    }

    #[test]
    fn lso_holds_at_first_txn_offset_until_commit_marker() {
        let tmp = tempfile::tempdir().unwrap();
        rt().block_on(async {
            let fs: Arc<dyn Fs> = Arc::new(RealFs::new());
            let p = Partition::open(
                fs,
                "t".into(),
                0,
                tmp.path().to_path_buf(),
                PartitionConfig::default(),
            )
            .await
            .unwrap();

            // Non-txn batch — LSO follows HWM.
            p.append(0, -1, build_batch(2, 1_000)).await.unwrap();
            assert_eq!(p.last_stable_offset(), 2);

            // Open a txn: 3 transactional records at offset 2.
            p.append(0, -1, build_txn_data_batch(42, 3)).await.unwrap();
            assert_eq!(p.high_watermark(), 5);
            assert_eq!(
                p.last_stable_offset(),
                2,
                "LSO must stay at the txn's first offset while open"
            );

            // Another txn data batch for the same PID — LSO unchanged.
            // (Same-pid second batch within the same open txn.)
            // We can't easily re-use the txn classifier path here
            // because the dedupe window would reject seq=0 a second
            // time; just skip and go straight to commit.
            let marker = build_marker_batch(42, true);
            p.append(0, -1, marker).await.unwrap();
            assert_eq!(p.high_watermark(), 6);
            assert_eq!(
                p.last_stable_offset(),
                p.high_watermark(),
                "LSO catches up to HWM once commit marker lands"
            );
            assert!(
                p.aborted_in_range(0, i64::MAX).is_empty(),
                "commit doesn't populate the aborted list"
            );
            p.close().await.unwrap();
        });
    }

    #[test]
    fn abort_marker_populates_aborted_list_with_first_offset() {
        let tmp = tempfile::tempdir().unwrap();
        rt().block_on(async {
            let fs: Arc<dyn Fs> = Arc::new(RealFs::new());
            let p = Partition::open(
                fs,
                "t".into(),
                0,
                tmp.path().to_path_buf(),
                PartitionConfig::default(),
            )
            .await
            .unwrap();
            p.append(0, -1, build_txn_data_batch(42, 3)).await.unwrap();
            p.append(0, -1, build_marker_batch(42, false))
                .await
                .unwrap();
            let aborted = p.aborted_in_range(0, i64::MAX);
            assert_eq!(aborted.len(), 1);
            assert_eq!(aborted[0].producer_id, 42);
            assert_eq!(aborted[0].first_offset, 0);
            assert_eq!(p.last_stable_offset(), p.high_watermark());
            p.close().await.unwrap();
        });
    }

    #[test]
    fn lso_picks_lowest_across_concurrent_open_txns() {
        let tmp = tempfile::tempdir().unwrap();
        rt().block_on(async {
            let fs: Arc<dyn Fs> = Arc::new(RealFs::new());
            let p = Partition::open(
                fs,
                "t".into(),
                0,
                tmp.path().to_path_buf(),
                PartitionConfig::default(),
            )
            .await
            .unwrap();
            // pid 1's first batch at offset 0
            p.append(0, -1, build_txn_data_batch(1, 2)).await.unwrap();
            // pid 2's first batch at offset 2
            p.append(0, -1, build_txn_data_batch(2, 4)).await.unwrap();
            assert_eq!(p.last_stable_offset(), 0, "min of {{0, 2}}");

            // Commit pid 1 → LSO jumps to pid 2's first offset.
            p.append(0, -1, build_marker_batch(1, true)).await.unwrap();
            assert_eq!(p.last_stable_offset(), 2);

            // Commit pid 2 → LSO catches HWM.
            p.append(0, -1, build_marker_batch(2, true)).await.unwrap();
            assert_eq!(p.last_stable_offset(), p.high_watermark());
            p.close().await.unwrap();
        });
    }

    #[test]
    fn open_fresh_partition_returns_zero_hwm() {
        let tmp = tempfile::tempdir().unwrap();
        rt().block_on(async {
            let fs: Arc<dyn Fs> = Arc::new(RealFs::new());
            let p = Partition::open(
                fs,
                "t".into(),
                0,
                tmp.path().to_path_buf(),
                PartitionConfig::default(),
            )
            .await
            .unwrap();
            assert_eq!(p.high_watermark(), 0);
            assert_eq!(p.log_start_offset(), 0);
            assert_eq!(p.epoch(), 0);
            p.close().await.unwrap();
        });
    }

    #[test]
    fn append_advances_hwm_and_returns_assigned_offset() {
        let tmp = tempfile::tempdir().unwrap();
        rt().block_on(async {
            let fs: Arc<dyn Fs> = Arc::new(RealFs::new());
            let p = Partition::open(
                fs,
                "t".into(),
                0,
                tmp.path().to_path_buf(),
                PartitionConfig::default(),
            )
            .await
            .unwrap();
            let base = p.append(0, -1, build_batch(5, 1_000)).await.unwrap();
            assert_eq!(base, 0);
            assert_eq!(p.high_watermark(), 5);
            let base = p.append(0, -1, build_batch(3, 1_000)).await.unwrap();
            assert_eq!(base, 5);
            assert_eq!(p.high_watermark(), 8);
            p.close().await.unwrap();
        });
    }

    #[test]
    fn acks_all_waits_for_committer() {
        let tmp = tempfile::tempdir().unwrap();
        rt().block_on(async {
            let fs: Arc<dyn Fs> = Arc::new(RealFs::new());
            let p = Partition::open(
                fs,
                "t".into(),
                0,
                tmp.path().to_path_buf(),
                PartitionConfig {
                    flush_interval_messages: 1,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
            // acks=-1 must not return before the committer fsyncs.
            // Hard to assert ordering without injecting a clock, but
            // at minimum the call must not deadlock and must reflect
            // a non-zero completed_flush_seq.
            p.append(0, -1, build_batch(1, 1_000)).await.unwrap();
            assert!(p.inner.lock().completed_flush_seq >= 1);
            p.close().await.unwrap();
        });
    }

    #[test]
    fn acks_all_below_interval_does_not_wait_for_any_flush() {
        // gh #188: with the durability dial raised
        // (flush_interval_messages ≫ 1), an acks=all append that does
        // NOT cross the threshold must ack immediately — even if a
        // flush is pending or in flight — mirroring the v0.1 engine's
        // `triggeredFlushSeq <= 0 → no wait`. The pre-fix shape parked
        // every acks=all append on the last-requested seq, stalling
        // the whole pipeline for the fsync window each cycle.
        let tmp = tempfile::tempdir().unwrap();
        rt().block_on(async {
            let fs: Arc<dyn Fs> = Arc::new(RealFs::new());
            let p = Partition::open(
                fs,
                "t".into(),
                0,
                tmp.path().to_path_buf(),
                PartitionConfig {
                    flush_interval_messages: 1_000,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
            // Two 5-record acks=all batches: far below the interval,
            // so no flush is ever requested — the appends must return
            // without any committer round trip.
            p.append(0, -1, build_batch(5, 1_000)).await.unwrap();
            p.append(0, -1, build_batch(5, 1_000)).await.unwrap();
            assert_eq!(p.inner.lock().requested_flush_seq, 0);
            assert_eq!(p.inner.lock().completed_flush_seq, 0);
            p.close().await.unwrap();
        });
    }

    #[test]
    fn acks_all_crossing_interval_still_waits() {
        // The other half of the gh #188 contract: the append that
        // crosses the threshold DOES wait for its fsync.
        let tmp = tempfile::tempdir().unwrap();
        rt().block_on(async {
            let fs: Arc<dyn Fs> = Arc::new(RealFs::new());
            let p = Partition::open(
                fs,
                "t".into(),
                0,
                tmp.path().to_path_buf(),
                PartitionConfig {
                    flush_interval_messages: 8,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
            p.append(0, -1, build_batch(5, 1_000)).await.unwrap();
            assert_eq!(p.inner.lock().completed_flush_seq, 0);
            // 5 + 5 crosses 8 → this append triggers seq 1 and must
            // not return before the committer has fsynced it.
            p.append(0, -1, build_batch(5, 1_000)).await.unwrap();
            assert!(p.inner.lock().completed_flush_seq >= 1);
            p.close().await.unwrap();
        });
    }

    #[test]
    fn flush_interval_messages_zero_means_no_committer_trigger() {
        let tmp = tempfile::tempdir().unwrap();
        rt().block_on(async {
            let fs: Arc<dyn Fs> = Arc::new(RealFs::new());
            let p = Partition::open(
                fs,
                "t".into(),
                0,
                tmp.path().to_path_buf(),
                PartitionConfig {
                    flush_interval_messages: 0,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
            // acks=0 → don't wait. With flush_interval=0 the committer
            // never fires.
            p.append(0, 0, build_batch(1, 1_000)).await.unwrap();
            assert_eq!(p.inner.lock().requested_flush_seq, 0);
            assert_eq!(p.inner.lock().completed_flush_seq, 0);
            p.close().await.unwrap();
        });
    }

    #[test]
    fn epoch_fence_rejects_stale_writes() {
        let tmp = tempfile::tempdir().unwrap();
        rt().block_on(async {
            let fs: Arc<dyn Fs> = Arc::new(RealFs::new());
            let p = Partition::open(
                fs,
                "t".into(),
                0,
                tmp.path().to_path_buf(),
                PartitionConfig::default(),
            )
            .await
            .unwrap();
            // Bump the manifest's epoch manually under the lock.
            {
                let mut guard = p.inner.lock();
                guard.epoch = 5;
            }
            let err = p.append(2, -1, build_batch(1, 1_000)).await.unwrap_err();
            assert!(matches!(err, StorageError::EpochMismatch));
            p.close().await.unwrap();
        });
    }

    #[test]
    fn read_returns_batches_from_start_offset() {
        let tmp = tempfile::tempdir().unwrap();
        rt().block_on(async {
            let fs: Arc<dyn Fs> = Arc::new(RealFs::new());
            let p = Partition::open(
                fs,
                "t".into(),
                0,
                tmp.path().to_path_buf(),
                PartitionConfig::default(),
            )
            .await
            .unwrap();
            for _ in 0..3 {
                p.append(0, -1, build_batch(1, 1_000)).await.unwrap();
            }
            // Read from offset 1 — should skip the first batch.
            let got = p.read(1, 4096).await.unwrap();
            let one_len = build_batch(1, 1_000).len();
            assert_eq!(got.len(), one_len * 2);
            p.close().await.unwrap();
        });
    }

    #[test]
    fn delete_records_advances_log_start() {
        let tmp = tempfile::tempdir().unwrap();
        rt().block_on(async {
            let fs: Arc<dyn Fs> = Arc::new(RealFs::new());
            let p = Partition::open(
                fs,
                "t".into(),
                0,
                tmp.path().to_path_buf(),
                PartitionConfig::default(),
            )
            .await
            .unwrap();
            for _ in 0..5 {
                p.append(0, -1, build_batch(1, 1_000)).await.unwrap();
            }
            let new_start = p.delete_records(3).await.unwrap();
            assert_eq!(new_start, 3);
            assert_eq!(p.log_start_offset(), 3);
            // Read clamps effective_start to log_start.
            let got = p.read(0, 4096).await.unwrap();
            let one_len = build_batch(1, 1_000).len();
            assert_eq!(got.len(), one_len * 2);
            p.close().await.unwrap();
        });
    }

    #[test]
    fn delete_records_purge_to_hwm() {
        let tmp = tempfile::tempdir().unwrap();
        rt().block_on(async {
            let fs: Arc<dyn Fs> = Arc::new(RealFs::new());
            let p = Partition::open(
                fs,
                "t".into(),
                0,
                tmp.path().to_path_buf(),
                PartitionConfig::default(),
            )
            .await
            .unwrap();
            for _ in 0..4 {
                p.append(0, -1, build_batch(1, 1_000)).await.unwrap();
            }
            let new_start = p.delete_records(-1).await.unwrap();
            assert_eq!(new_start, 4);
            p.close().await.unwrap();
        });
    }

    #[test]
    fn delete_records_past_hwm_is_offset_out_of_range() {
        let tmp = tempfile::tempdir().unwrap();
        rt().block_on(async {
            let fs: Arc<dyn Fs> = Arc::new(RealFs::new());
            let p = Partition::open(
                fs,
                "t".into(),
                0,
                tmp.path().to_path_buf(),
                PartitionConfig::default(),
            )
            .await
            .unwrap();
            p.append(0, -1, build_batch(1, 1_000)).await.unwrap();
            let err = p.delete_records(999).await.unwrap_err();
            assert!(matches!(err, StorageError::OffsetOutOfRange));
            p.close().await.unwrap();
        });
    }

    #[test]
    fn reopen_recovers_hwm_and_log_start_from_manifest() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_path_buf();
        rt().block_on(async {
            let fs: Arc<dyn Fs> = Arc::new(RealFs::new());
            // First open + append + delete_records + close.
            let p = Partition::open(
                fs.clone(),
                "t".into(),
                0,
                dir.clone(),
                PartitionConfig::default(),
            )
            .await
            .unwrap();
            for _ in 0..5 {
                p.append(0, -1, build_batch(1, 1_000)).await.unwrap();
            }
            p.delete_records(2).await.unwrap();
            p.close().await.unwrap();

            // Reopen — HWM + log_start should come from manifest.
            let p2 = Partition::open(fs, "t".into(), 0, dir.clone(), PartitionConfig::default())
                .await
                .unwrap();
            assert_eq!(p2.high_watermark(), 5);
            assert_eq!(p2.log_start_offset(), 2);
            p2.close().await.unwrap();
        });
    }

    /// gh #220: an ENOENT during open means the directory is being
    /// re-created underneath us (the operator's reclaim renames it aside
    /// and mkdirs it again), never that the path is wrong — `open`
    /// mkdir_all's it first. Open must ride that out instead of handing
    /// the caller a produce error or a partition that stays closed until
    /// the next takeover reconcile.
    #[test]
    fn open_rides_out_a_vanishing_directory() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("t").join("0");
        rt().block_on(async {
            let fs: Arc<dyn Fs> = Arc::new(VanishingFs {
                inner: RealFs::new(),
                fail_creates: std::sync::atomic::AtomicU32::new(2),
            });
            let p = Partition::open(fs, "t".into(), 0, dir.clone(), PartitionConfig::default())
                .await
                .expect("open should retry past a transient ENOENT");
            // And the partition is fully usable, not a husk.
            p.append(0, -1, build_batch(1, 1_000)).await.unwrap();
            assert_eq!(p.high_watermark(), 1);
        });
    }

    /// The retry is bounded — a directory that never comes back still
    /// surfaces the error rather than hanging the open forever.
    #[test]
    fn open_gives_up_on_a_permanently_missing_directory() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("t").join("0");
        rt().block_on(async {
            let fs: Arc<dyn Fs> = Arc::new(VanishingFs {
                inner: RealFs::new(),
                fail_creates: std::sync::atomic::AtomicU32::new(u32::MAX),
            });
            let err = Partition::open(fs, "t".into(), 0, dir, PartitionConfig::default())
                .await
                .expect_err("a permanent ENOENT must surface");
            assert!(matches!(err, StorageError::Io(e) if e.kind() == std::io::ErrorKind::NotFound));
        });
    }

    /// gh #219: `abandon` must not write state back. The scenario is a
    /// topic delete→recreate: the operator stages the old directory
    /// aside and a fresh one appears at the same path, so anything the
    /// dying partition persists lands in the *new* incarnation's dir
    /// (an advanced HWM over an empty log, plus a poisoned idempotence
    /// window).
    #[test]
    fn abandon_persists_nothing_into_a_recreated_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("t").join("0");
        rt().block_on(async {
            let fs: Arc<dyn Fs> = Arc::new(RealFs::new());
            let p = Partition::open(
                fs.clone(),
                "t".into(),
                0,
                dir.clone(),
                PartitionConfig::default(),
            )
            .await
            .unwrap();
            for _ in 0..5 {
                p.append(0, -1, build_batch(1, 1_000)).await.unwrap();
            }
            assert_eq!(p.high_watermark(), 5);

            // Stand in for the operator's reclaim: the directory this
            // partition was opened on is replaced by an empty one.
            std::fs::remove_dir_all(tmp.path().join("t")).unwrap();
            std::fs::create_dir_all(&dir).unwrap();

            p.abandon().await;

            let leftovers: Vec<String> = std::fs::read_dir(&dir)
                .unwrap()
                .flatten()
                .map(|e| e.file_name().to_string_lossy().to_string())
                .collect();
            assert!(
                leftovers.is_empty(),
                "abandon must leave the recreated dir untouched, found {leftovers:?}"
            );

            // And the recreated partition starts from zero.
            let fresh = Partition::open(fs, "t".into(), 0, dir.clone(), PartitionConfig::default())
                .await
                .unwrap();
            assert_eq!(fresh.high_watermark(), 0);
            assert_eq!(fresh.log_start_offset(), 0);
        });
    }

    /// gh #recovery-checkpoint: a clean close checkpoints the active
    /// segment at EOF, and reopen recovers the HWM from it.
    #[test]
    fn close_writes_checkpoint_at_eof_and_reopen_recovers() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_path_buf();
        rt().block_on(async {
            let fs: Arc<dyn Fs> = Arc::new(RealFs::new());
            let p = Partition::open(
                fs.clone(),
                "t".into(),
                0,
                dir.clone(),
                PartitionConfig::default(),
            )
            .await
            .unwrap();
            for _ in 0..5 {
                p.append(0, -1, build_batch(1, 1_000)).await.unwrap();
            }
            p.close().await.unwrap();

            // The checkpoint exists, sits at EOF, and carries the HWM.
            let cp = recovery_checkpoint::read(fs.as_ref(), &dir)
                .expect("clean close writes a recovery checkpoint");
            assert_eq!(cp.high_watermark, 5);
            assert_eq!(cp.segment_base, 0);
            let log_len = std::fs::metadata(dir.join("00000000-00000000000000000000.log"))
                .unwrap()
                .len();
            assert_eq!(
                u64::try_from(cp.byte_pos).unwrap(),
                log_len,
                "checkpoint at EOF"
            );

            let p2 = Partition::open(fs, "t".into(), 0, dir.clone(), PartitionConfig::default())
                .await
                .unwrap();
            assert_eq!(p2.high_watermark(), 5);
            p2.close().await.unwrap();
        });
    }

    /// The checkpoint guard: a checkpoint that names a *different*
    /// active segment is ignored, and recovery falls back to a correct
    /// full scan.
    #[test]
    fn stale_checkpoint_for_other_segment_is_ignored() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_path_buf();
        rt().block_on(async {
            let fs: Arc<dyn Fs> = Arc::new(RealFs::new());
            let p = Partition::open(
                fs.clone(),
                "t".into(),
                0,
                dir.clone(),
                PartitionConfig::default(),
            )
            .await
            .unwrap();
            for _ in 0..3 {
                p.append(0, -1, build_batch(1, 1_000)).await.unwrap();
            }
            p.close().await.unwrap();

            // Poison the checkpoint: claim a segment base that doesn't
            // match, with a bogus HWM. Recovery must ignore it.
            recovery_checkpoint::write(
                fs.as_ref(),
                &dir,
                &RecoveryCheckpoint {
                    segment_base: 999,
                    byte_pos: 0,
                    high_watermark: 12345,
                },
            )
            .unwrap();

            let p2 = Partition::open(fs, "t".into(), 0, dir.clone(), PartitionConfig::default())
                .await
                .unwrap();
            assert_eq!(
                p2.high_watermark(),
                3,
                "fell back to a full scan, not the bogus HWM"
            );
            p2.close().await.unwrap();
        });
    }

    #[test]
    fn segment_roll_at_size_threshold() {
        let tmp = tempfile::tempdir().unwrap();
        rt().block_on(async {
            let fs: Arc<dyn Fs> = Arc::new(RealFs::new());
            let one_len = u64::try_from(build_batch(1, 1_000).len()).unwrap();
            let p = Partition::open(
                fs,
                "t".into(),
                0,
                tmp.path().to_path_buf(),
                PartitionConfig {
                    // Roll after one batch.
                    segment_bytes: one_len,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
            for _ in 0..3 {
                p.append(0, -1, build_batch(1, 1_000)).await.unwrap();
            }
            // After 3 appends with segment_bytes == one batch, we
            // should have at least 1 closed segment.
            assert!(
                !p.inner.lock().closed.is_empty(),
                "expected at least one roll"
            );
            p.close().await.unwrap();
        });
    }

    #[test]
    fn high_watermark_read_is_lock_free() {
        // Smoke-test: while a background appender holds the inner
        // mutex briefly, high_watermark() returns without blocking.
        let tmp = tempfile::tempdir().unwrap();
        rt().block_on(async {
            let fs: Arc<dyn Fs> = Arc::new(RealFs::new());
            let p = Arc::new(
                Partition::open(
                    fs,
                    "t".into(),
                    0,
                    tmp.path().to_path_buf(),
                    PartitionConfig::default(),
                )
                .await
                .unwrap(),
            );
            // Fire 10 concurrent reads — they must not deadlock with
            // the append path or each other.
            let mut handles = Vec::new();
            for _ in 0..10 {
                let p2 = p.clone();
                handles.push(tokio::spawn(async move { p2.high_watermark() }));
            }
            for h in handles {
                let _ = h.await.unwrap();
            }
            p.close().await.unwrap();
        });
    }
}
