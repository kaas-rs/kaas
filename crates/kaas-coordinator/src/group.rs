//! Consumer-group state machine.
//!
//! State transitions
//! mirror Apache Kafka's `GroupCoordinator`:
//!
//! ```text
//! Empty → PreparingRebalance → CompletingRebalance → Stable
//!   ↑                            ↓                    ↓
//!   └─────────────────────── (last leave)   (new join → PreparingRebalance)
//! ```
//!
//! The blocking
//! `join` / `sync` waiters are per-member
//! `oneshot::Sender<...>` slots stored against the join/sync waiter
//! lists; handler tasks `.await` the matching `oneshot::Receiver`.
//!
//! Timers (rebalance completion, heartbeat session timeout) are
//! tokio tasks awaiting `sleep_until(deadline)` and re-locking the
//! group's `Mutex` to mutate state on wake. The previous JoinHandle
//! is `.abort()`-ed on reset (timer-reset semantics).
//!
//! The coordinator surface lands incrementally. This cut covers:
//!
//! - Dynamic membership (anonymous `member_id=""` joining a new group).
//! - The gh #98 waiter-drain — `remove_member` wakes any pending
//!   joiner with `UNKNOWN_MEMBER_ID` so a session-timed-out leader
//!   doesn't strand its join() waiter.
//! - The gh #111 cold-start delay — initial rebalance extends by
//!   `INITIAL_REBALANCE_DELAY_MS` on each new arrival, capped at the
//!   max member `rebalance_timeout_ms`.
//! - Single-protocol "leader-first-mutual" select.
//! - Heartbeat session timeout → evict + bounce to PreparingRebalance.
//!
//! Deferred to a follow-up (clearly marked at each call site):
//!
//! - KIP-394 (MEMBER_ID_REQUIRED) at v4+ — pending-member registry.
//! - Static membership reconciliation across rebalances.
//! - gh #111 layer-4 empty-`ConsumerProtocolAssignment` safety net.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use parking_lot::Mutex;
use tokio::sync::oneshot;
use tokio::time::Instant;
use tracing::warn;

/// Kafka's `group.initial.rebalance.delay.ms` default.
///
/// Public for tests in this crate that drive a small-time-budget
/// rebalance. Production callers never mutate it.
pub const INITIAL_REBALANCE_DELAY_MS: u64 = 3_000;

/// Default member `rebalance_timeout_ms` when the client doesn't
/// declare one (Apache's 30 s default).
const DEFAULT_REBALANCE_TIMEOUT_MS: i32 = 30_000;

/// Default member `session_timeout_ms` when the client doesn't
/// declare one (Apache's 30 s default).
const DEFAULT_SESSION_TIMEOUT_MS: i32 = 30_000;

/// Apache error codes. The codec module doesn't carry these as
/// constants yet; the handler shells in `kaas-broker` will surface
/// them through `Response::error_code`.
pub mod error_codes {
    pub const NONE: i16 = 0;
    pub const UNKNOWN_MEMBER_ID: i16 = 25;
    pub const REBALANCE_IN_PROGRESS: i16 = 27;
    pub const ILLEGAL_GENERATION: i16 = 22;
    pub const MEMBER_ID_REQUIRED: i16 = 79;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GroupState {
    Empty,
    PreparingRebalance,
    CompletingRebalance,
    Stable,
    Dead,
}

impl GroupState {
    pub fn as_str(self) -> &'static str {
        match self {
            GroupState::Empty => "Empty",
            GroupState::PreparingRebalance => "PreparingRebalance",
            GroupState::CompletingRebalance => "CompletingRebalance",
            GroupState::Stable => "Stable",
            GroupState::Dead => "Dead",
        }
    }
}

/// A single protocol/assignor declared by a JoinGroup member.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProtocolMetadata {
    pub name: String,
    pub metadata: Bytes,
}

/// In-memory record of one consumer-group member.
#[derive(Debug)]
pub struct GroupMember {
    pub id: String,
    pub client_id: String,
    pub client_host: String,
    pub group_instance_id: Option<String>,
    pub session_timeout_ms: i32,
    pub rebalance_timeout_ms: i32,
    pub protocols: Vec<ProtocolMetadata>,
    /// Per-member assignment cached after a successful SyncGroup
    /// round. Empty until the leader publishes the round's
    /// assignments.
    pub assignment: Bytes,
    /// `JoinHandle` of the spawned session-timeout task, if any.
    /// Aborted on every heartbeat / fresh join.
    heartbeat_handle: Option<tokio::task::JoinHandle<()>>,
}

/// One JoinGroup waiter parked on a `oneshot::Receiver`.
#[derive(Debug)]
struct JoinWaiter {
    member_id: String,
    tx: oneshot::Sender<JoinOutcome>,
}

/// One in-flight SyncGroup round.
#[derive(Debug, Default)]
struct SyncRound {
    assignments: HashMap<String, Bytes>,
    /// `true` once the leader's SyncGroup completed (followers may
    /// read `assignments`).
    delivered: bool,
    /// `true` if this round was canceled by a fresh JoinGroup
    /// bumping the group back to `PreparingRebalance` before the
    /// leader stored assignments. Followers waking on this flag get
    /// `REBALANCE_IN_PROGRESS` rather than a 0-byte assignment
    /// (gh #111).
    canceled: bool,
    waiters: Vec<oneshot::Sender<()>>,
}

/// Outcome handed back to a parked JoinGroup waiter.
#[derive(Debug, Clone)]
pub struct JoinOutcome {
    pub error_code: i16,
    pub generation_id: i32,
    pub leader: String,
    pub member_id: String,
    pub protocol_type: String,
    pub protocol_name: String,
    pub members: Vec<JoinedMember>,
}

/// One entry in the leader's member-list payload returned by
/// JoinGroup.
#[derive(Debug, Clone)]
pub struct JoinedMember {
    pub member_id: String,
    pub group_instance_id: Option<String>,
    pub metadata: Bytes,
}

/// Public snapshot used by DescribeGroups.
#[derive(Debug, Clone)]
pub struct GroupSnapshot {
    pub id: String,
    pub state: &'static str,
    pub protocol_type: String,
    pub protocol_name: String,
    pub generation_id: i32,
    pub members: Vec<MemberSnapshot>,
}

#[derive(Debug, Clone)]
pub struct MemberSnapshot {
    pub member_id: String,
    pub client_id: String,
    pub group_instance_id: Option<String>,
    pub assignment: Bytes,
}

/// Request shape for `Group::join`. Field names mirror the codec
/// module's `JoinGroupRequest`.
#[derive(Debug, Clone)]
pub struct JoinRequest {
    pub member_id: String,
    pub group_instance_id: Option<String>,
    pub session_timeout_ms: i32,
    pub rebalance_timeout_ms: i32,
    pub protocol_type: String,
    pub protocols: Vec<ProtocolMetadata>,
    /// Wire version of the JoinGroupRequest — gates KIP-394
    /// (currently unused; the pending-members path is a follow-up).
    pub version: i16,
    pub client_id: String,
    pub client_host: String,
}

/// Request shape for `Group::sync`.
#[derive(Debug, Clone, Default)]
pub struct SyncRequest {
    pub member_id: String,
    pub generation_id: i32,
    pub group_instance_id: Option<String>,
    pub protocol_type: Option<String>,
    pub protocol_name: Option<String>,
    pub assignments: Vec<SyncAssignment>,
}

#[derive(Debug, Clone)]
pub struct SyncAssignment {
    pub member_id: String,
    pub assignment: Bytes,
}

/// Response shape from `Group::sync`.
#[derive(Debug, Clone, Default)]
pub struct SyncOutcome {
    pub error_code: i16,
    pub protocol_type: String,
    pub protocol_name: String,
    pub assignment: Bytes,
}

/// Request shape for `Group::heartbeat`.
#[derive(Debug, Clone)]
pub struct HeartbeatRequest {
    pub member_id: String,
    pub generation_id: i32,
    pub group_instance_id: Option<String>,
}

/// Per-group state held under a single `Mutex` — every public method
/// re-acquires it (coarse-grained on purpose).
pub struct Group {
    state: Mutex<GroupInner>,
}

struct GroupInner {
    id: String,
    state: GroupState,
    generation_id: i32,
    protocol_type: String,
    protocol_name: String,
    leader_id: String,
    members: HashMap<String, GroupMember>,
    join_waiters: Vec<JoinWaiter>,
    current_sync: Option<Arc<Mutex<SyncRound>>>,
    rebalance_handle: Option<tokio::task::JoinHandle<()>>,
    initial_rebalance: bool,
    initial_rebalance_deadline: Option<Instant>,
}

impl std::fmt::Debug for Group {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = self.state.lock();
        f.debug_struct("Group")
            .field("id", &s.id)
            .field("state", &s.state)
            .field("generation_id", &s.generation_id)
            .field("members", &s.members.keys().collect::<Vec<_>>())
            .finish()
    }
}

impl Group {
    pub fn new(id: impl Into<String>) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(GroupInner {
                id: id.into(),
                state: GroupState::Empty,
                generation_id: 0,
                protocol_type: String::new(),
                protocol_name: String::new(),
                leader_id: String::new(),
                members: HashMap::new(),
                join_waiters: Vec::new(),
                current_sync: None,
                rebalance_handle: None,
                initial_rebalance: true,
                initial_rebalance_deadline: None,
            }),
        })
    }

    pub fn id(&self) -> String {
        self.state.lock().id.clone()
    }

    pub fn state(&self) -> GroupState {
        self.state.lock().state
    }

    /// Snapshot the group for DescribeGroups.
    pub fn describe(&self) -> GroupSnapshot {
        let s = self.state.lock();
        GroupSnapshot {
            id: s.id.clone(),
            state: s.state.as_str(),
            protocol_type: s.protocol_type.clone(),
            protocol_name: s.protocol_name.clone(),
            generation_id: s.generation_id,
            members: s
                .members
                .values()
                .map(|m| MemberSnapshot {
                    member_id: m.id.clone(),
                    client_id: m.client_id.clone(),
                    group_instance_id: m.group_instance_id.clone(),
                    assignment: m.assignment.clone(),
                })
                .collect(),
        }
    }

    /// Drive a JoinGroup request through the state machine and await
    /// the rebalance-complete response. Returns the per-member
    /// `JoinOutcome`.
    pub async fn join(self: &Arc<Self>, req: JoinRequest) -> JoinOutcome {
        let (rx, member_id_for_err) = {
            let mut s = self.state.lock();

            // Assign a member id if absent. The KIP-394 v4+
            // MEMBER_ID_REQUIRED handshake lands in a follow-up;
            // Phase 5 takes the legacy "assign inline" path.
            let mut member_id = req.member_id.clone();
            if member_id.is_empty() {
                member_id = generate_member_id(&req.client_id);
            }

            // Upsert member.
            let is_new = !s.members.contains_key(&member_id);
            let m = s
                .members
                .entry(member_id.clone())
                .or_insert_with(|| GroupMember {
                    id: member_id.clone(),
                    client_id: req.client_id.clone(),
                    client_host: req.client_host.clone(),
                    group_instance_id: req.group_instance_id.clone(),
                    session_timeout_ms: req.session_timeout_ms,
                    rebalance_timeout_ms: req.rebalance_timeout_ms,
                    protocols: req.protocols.clone(),
                    assignment: Bytes::new(),
                    heartbeat_handle: None,
                });
            m.group_instance_id = req.group_instance_id.clone();
            m.session_timeout_ms = req.session_timeout_ms;
            m.rebalance_timeout_ms = req.rebalance_timeout_ms;
            m.protocols = req.protocols.clone();
            if !req.protocol_type.is_empty() {
                s.protocol_type = req.protocol_type.clone();
            }

            // (Re)arm the session-timeout watchdog for this member.
            self.reset_heartbeat_timer(&mut s, &member_id);

            // State machine transitions.
            match s.state {
                GroupState::Empty => {
                    s.state = GroupState::PreparingRebalance;
                    let cap = Duration::from_millis(
                        u64::try_from(max_rebalance_timeout(&s)).unwrap_or(30_000),
                    );
                    s.initial_rebalance_deadline = Some(Instant::now() + cap);
                    self.start_rebalance_timer(&mut s, true);
                }
                GroupState::Stable | GroupState::CompletingRebalance => {
                    s.state = GroupState::PreparingRebalance;
                    self.start_rebalance_timer(&mut s, false);
                    // Cancel any in-flight sync so blocked SyncGroup
                    // calls unblock with REBALANCE_IN_PROGRESS.
                    if let Some(ss) = s.current_sync.take() {
                        cancel_sync(&ss);
                    }
                }
                GroupState::PreparingRebalance => {
                    if s.initial_rebalance && is_new {
                        self.start_rebalance_timer(&mut s, true);
                    }
                }
                GroupState::Dead => {
                    // Race: group was relinquished while a late JoinGroup
                    // arrived. Surface UNKNOWN_MEMBER_ID so the client
                    // re-bootstraps.
                    return JoinOutcome {
                        error_code: error_codes::UNKNOWN_MEMBER_ID,
                        generation_id: 0,
                        leader: String::new(),
                        member_id,
                        protocol_type: String::new(),
                        protocol_name: String::new(),
                        members: Vec::new(),
                    };
                }
            }

            let (tx, rx) = oneshot::channel::<JoinOutcome>();
            s.join_waiters.push(JoinWaiter {
                member_id: member_id.clone(),
                tx,
            });
            self.maybe_complete_rebalance(&mut s);
            (rx, member_id)
        };

        match rx.await {
            Ok(out) => out,
            Err(_) => JoinOutcome {
                // Channel dropped without delivery — group was shut
                // down → UNKNOWN_MEMBER_ID.
                error_code: error_codes::UNKNOWN_MEMBER_ID,
                generation_id: 0,
                leader: String::new(),
                member_id: member_id_for_err,
                protocol_type: String::new(),
                protocol_name: String::new(),
                members: Vec::new(),
            },
        }
    }

    /// Drive a SyncGroup request. The leader publishes assignments
    /// and unblocks every follower; followers park on the round's
    /// waiter channel until the leader (or a cancel) wakes them.
    pub async fn sync(self: &Arc<Self>, req: SyncRequest) -> SyncOutcome {
        let (round, member_id, protocol_type, protocol_name, rx) = {
            let mut s = self.state.lock();

            if !s.members.contains_key(&req.member_id) {
                return SyncOutcome {
                    error_code: error_codes::UNKNOWN_MEMBER_ID,
                    ..SyncOutcome::default()
                };
            }
            if req.generation_id != s.generation_id {
                return SyncOutcome {
                    error_code: error_codes::ILLEGAL_GENERATION,
                    ..SyncOutcome::default()
                };
            }
            // gh #111: followers' SyncGroup is valid in both
            // CompletingRebalance (waiting for leader) AND Stable
            // (leader already finished first).
            if !matches!(
                s.state,
                GroupState::CompletingRebalance | GroupState::Stable
            ) {
                return SyncOutcome {
                    error_code: error_codes::REBALANCE_IN_PROGRESS,
                    ..SyncOutcome::default()
                };
            }

            let round = match s.current_sync.clone() {
                Some(r) => r,
                None => {
                    return SyncOutcome {
                        error_code: error_codes::REBALANCE_IN_PROGRESS,
                        ..SyncOutcome::default()
                    };
                }
            };
            let protocol_type = s.protocol_type.clone();
            let protocol_name = s.protocol_name.clone();
            let is_leader = req.member_id == s.leader_id;

            if is_leader {
                let mut r = round.lock();
                for a in &req.assignments {
                    r.assignments
                        .insert(a.member_id.clone(), a.assignment.clone());
                }
                // Members the leader omitted get an empty assignment.
                // Phase-5 cut writes empty Bytes; the gh #111 layer-4
                // "valid empty ConsumerProtocolAssignment" safety net
                // is a follow-up.
                let m_keys: Vec<String> = s.members.keys().cloned().collect();
                for mid in m_keys {
                    r.assignments.entry(mid).or_insert_with(Bytes::new);
                }
                r.delivered = true;
                s.state = GroupState::Stable;
                // Consumer-group rebalance completed — every trip
                // through PreparingRebalance → CompletingRebalance →
                // Stable is one datapoint. Alert fires on
                // rate(rebalances[5m]) > threshold.
                kaas_observability::metrics::global()
                    .group_rebalances
                    .add(1, &[]);
                // Wake all parked followers.
                let waiters = std::mem::take(&mut r.waiters);
                for w in waiters {
                    let _ = w.send(());
                }
            }
            let (tx, rx) = oneshot::channel::<()>();
            {
                let mut r = round.lock();
                if r.delivered || r.canceled {
                    // Leader already finished (or round canceled) —
                    // resolve immediately rather than parking.
                    let _ = tx.send(());
                } else {
                    r.waiters.push(tx);
                }
            }
            (
                round,
                req.member_id.clone(),
                protocol_type,
                protocol_name,
                rx,
            )
        };

        let _ = rx.await;

        let r = round.lock();
        if r.canceled {
            SyncOutcome {
                error_code: error_codes::REBALANCE_IN_PROGRESS,
                ..SyncOutcome::default()
            }
        } else {
            let assignment = r.assignments.get(&member_id).cloned().unwrap_or_default();
            SyncOutcome {
                error_code: error_codes::NONE,
                protocol_type,
                protocol_name,
                assignment,
            }
        }
    }

    /// Heartbeat against the group. Returns the wire-level error code.
    pub fn heartbeat(self: &Arc<Self>, req: HeartbeatRequest) -> i16 {
        let mut s = self.state.lock();
        if matches!(s.state, GroupState::Empty | GroupState::Dead) {
            return error_codes::UNKNOWN_MEMBER_ID;
        }
        if !s.members.contains_key(&req.member_id) {
            return error_codes::UNKNOWN_MEMBER_ID;
        }
        if req.generation_id != s.generation_id {
            return error_codes::ILLEGAL_GENERATION;
        }
        self.reset_heartbeat_timer(&mut s, &req.member_id);
        match s.state {
            GroupState::PreparingRebalance | GroupState::CompletingRebalance => {
                error_codes::REBALANCE_IN_PROGRESS
            }
            _ => error_codes::NONE,
        }
    }

    /// LeaveGroup for the given members. Returns per-member error
    /// codes paired with member_id.
    pub fn leave(self: &Arc<Self>, member_ids: &[String]) -> Vec<(String, i16)> {
        let mut s = self.state.lock();
        let mut out = Vec::with_capacity(member_ids.len());
        for mid in member_ids {
            if !s.members.contains_key(mid) {
                out.push((mid.clone(), error_codes::UNKNOWN_MEMBER_ID));
                continue;
            }
            self.remove_member(&mut s, mid);
            out.push((mid.clone(), error_codes::NONE));
        }
        if s.members.is_empty() {
            s.state = GroupState::Empty;
            if let Some(h) = s.rebalance_handle.take() {
                h.abort();
            }
        } else if matches!(s.state, GroupState::Stable) {
            s.state = GroupState::PreparingRebalance;
            self.start_rebalance_timer(&mut s, false);
        }
        out
    }

    /// Shutdown a group — fires every parked waiter with
    /// UNKNOWN_MEMBER_ID, aborts timers, and transitions to Dead.
    /// Called from `Manager::relinquish_group`.
    pub fn shutdown(self: &Arc<Self>) {
        let mut s = self.state.lock();
        let mids: Vec<String> = s.members.keys().cloned().collect();
        for mid in mids {
            self.remove_member(&mut s, &mid);
        }
        if let Some(h) = s.rebalance_handle.take() {
            h.abort();
        }
        if let Some(ss) = s.current_sync.take() {
            cancel_sync(&ss);
        }
        s.state = GroupState::Dead;
        s.members.clear();
    }

    // ----- private helpers -------------------------------------------------

    fn maybe_complete_rebalance(self: &Arc<Self>, s: &mut GroupInner) {
        if s.initial_rebalance {
            // Initial rebalance never completes early — rely on the
            // timer so late-arriving cold-start members register
            // before the leader is picked (gh #111).
            return;
        }
        if s.state == GroupState::PreparingRebalance && s.join_waiters.len() >= s.members.len() {
            self.complete_rebalance(s);
        }
    }

    fn complete_rebalance(self: &Arc<Self>, s: &mut GroupInner) {
        if let Some(h) = s.rebalance_handle.take() {
            h.abort();
        }
        s.state = GroupState::CompletingRebalance;
        s.initial_rebalance = false;
        s.initial_rebalance_deadline = None;
        s.generation_id += 1;
        let waiters = std::mem::take(&mut s.join_waiters);
        // Default selectProtocol -> first protocol declared by the
        // leader that every other waiter also lists.
        s.protocol_name = select_protocol(&s.members, &waiters).unwrap_or_default();
        if let Some(first) = waiters.first() {
            s.leader_id = first.member_id.clone();
        }
        // Fresh sync round.
        let sync = Arc::new(Mutex::new(SyncRound::default()));
        s.current_sync = Some(sync.clone());

        let leader_id = s.leader_id.clone();
        let generation = s.generation_id;
        let protocol_type = s.protocol_type.clone();
        let protocol_name = s.protocol_name.clone();

        // Build the leader's member-list payload once (it carries
        // each waiter's protocol metadata).
        let leader_payload: Vec<JoinedMember> = waiters
            .iter()
            .map(|w| {
                let m = s.members.get(&w.member_id);
                let group_instance_id = m.and_then(|m| m.group_instance_id.clone());
                let metadata = m
                    .and_then(|m| {
                        m.protocols
                            .iter()
                            .find(|p| p.name == protocol_name)
                            .map(|p| p.metadata.clone())
                    })
                    .unwrap_or_default();
                JoinedMember {
                    member_id: w.member_id.clone(),
                    group_instance_id,
                    metadata,
                }
            })
            .collect();

        // Deliver the outcome to every parked join() future.
        for w in waiters {
            let members_payload = if w.member_id == leader_id {
                leader_payload.clone()
            } else {
                Vec::new()
            };
            let outcome = JoinOutcome {
                error_code: error_codes::NONE,
                generation_id: generation,
                leader: leader_id.clone(),
                member_id: w.member_id.clone(),
                protocol_type: protocol_type.clone(),
                protocol_name: protocol_name.clone(),
                members: members_payload,
            };
            let _ = w.tx.send(outcome);
        }
    }

    fn start_rebalance_timer(self: &Arc<Self>, s: &mut GroupInner, initial: bool) {
        let max_ms = max_rebalance_timeout(s);
        let mut budget_ms = max_ms;
        if initial && budget_ms > i32::try_from(INITIAL_REBALANCE_DELAY_MS).unwrap_or(i32::MAX) {
            budget_ms = i32::try_from(INITIAL_REBALANCE_DELAY_MS).unwrap_or(i32::MAX);
        }
        if initial {
            if let Some(deadline) = s.initial_rebalance_deadline {
                let remaining = deadline.saturating_duration_since(Instant::now());
                let remaining_ms = i32::try_from(remaining.as_millis()).unwrap_or(i32::MAX);
                if budget_ms > remaining_ms {
                    budget_ms = remaining_ms;
                }
            }
        }
        if budget_ms < 0 {
            budget_ms = 0;
        }
        if let Some(h) = s.rebalance_handle.take() {
            h.abort();
        }
        let group = self.clone();
        let wait = Duration::from_millis(u64::try_from(budget_ms).unwrap_or(0));
        let handle = tokio::spawn(async move {
            tokio::time::sleep(wait).await;
            let mut s = group.state.lock();
            if s.state == GroupState::PreparingRebalance && !s.join_waiters.is_empty() {
                group.evict_non_rejoining_members(&mut s);
                group.complete_rebalance(&mut s);
            }
        });
        s.rebalance_handle = Some(handle);
    }

    fn reset_heartbeat_timer(self: &Arc<Self>, s: &mut GroupInner, member_id: &str) {
        let timeout_ms = s
            .members
            .get(member_id)
            .map(|m| m.session_timeout_ms)
            .unwrap_or(DEFAULT_SESSION_TIMEOUT_MS);
        let timeout_ms = if timeout_ms <= 0 {
            DEFAULT_SESSION_TIMEOUT_MS
        } else {
            timeout_ms
        };
        let wait = Duration::from_millis(u64::try_from(timeout_ms).unwrap_or(30_000));
        if let Some(m) = s.members.get_mut(member_id) {
            if let Some(h) = m.heartbeat_handle.take() {
                h.abort();
            }
        }
        let group = self.clone();
        let mid = member_id.to_owned();
        let handle = tokio::spawn(async move {
            tokio::time::sleep(wait).await;
            let mut s = group.state.lock();
            if !s.members.contains_key(&mid) {
                return;
            }
            group.remove_member(&mut s, &mid);
            if s.members.is_empty() {
                s.state = GroupState::Empty;
                if let Some(h) = s.rebalance_handle.take() {
                    h.abort();
                }
            } else if matches!(s.state, GroupState::Stable) {
                s.state = GroupState::PreparingRebalance;
                group.start_rebalance_timer(&mut s, false);
            }
        });
        if let Some(m) = s.members.get_mut(member_id) {
            m.heartbeat_handle = Some(handle);
        }
    }

    /// Idempotent. Removes the member from the map, drains any
    /// matching JoinGroup waiter (gh #98 #1) so the parked future
    /// wakes with UNKNOWN_MEMBER_ID, aborts the session timer.
    fn remove_member(self: &Arc<Self>, s: &mut GroupInner, mid: &str) {
        if let Some(m) = s.members.get_mut(mid) {
            if let Some(h) = m.heartbeat_handle.take() {
                h.abort();
            }
        }
        s.members.remove(mid);
        // Drain matching waiter — fire it with UNKNOWN_MEMBER_ID so
        // the join() future doesn't sit on a dropped sender.
        if let Some(idx) = s.join_waiters.iter().position(|w| w.member_id == mid) {
            let w = s.join_waiters.remove(idx);
            let _ = w.tx.send(JoinOutcome {
                error_code: error_codes::UNKNOWN_MEMBER_ID,
                generation_id: 0,
                leader: String::new(),
                member_id: mid.to_owned(),
                protocol_type: String::new(),
                protocol_name: String::new(),
                members: Vec::new(),
            });
        }
    }

    /// Drop dynamic members that didn't issue a JoinGroup during this
    /// rebalance round — Apache's `onCompleteJoin` parity (gh #113).
    /// Static members survive a missed rebalance.
    fn evict_non_rejoining_members(self: &Arc<Self>, s: &mut GroupInner) {
        if s.members.len() == s.join_waiters.len() {
            return;
        }
        let rejoined: std::collections::HashSet<String> =
            s.join_waiters.iter().map(|w| w.member_id.clone()).collect();
        let stale: Vec<String> = s
            .members
            .iter()
            .filter(|(mid, m)| !rejoined.contains(*mid) && m.group_instance_id.is_none())
            .map(|(mid, _)| mid.clone())
            .collect();
        for mid in stale {
            warn!(
                group = s.id.as_str(),
                member = mid.as_str(),
                generation = s.generation_id,
                "rebalance: evicting dynamic member that did not rejoin within rebalance_timeout_ms"
            );
            self.remove_member(s, &mid);
        }
    }
}

fn cancel_sync(ss: &Arc<Mutex<SyncRound>>) {
    let mut r = ss.lock();
    if r.delivered || r.canceled {
        return;
    }
    r.canceled = true;
    let waiters = std::mem::take(&mut r.waiters);
    for w in waiters {
        let _ = w.send(());
    }
}

fn max_rebalance_timeout(s: &GroupInner) -> i32 {
    let mut max = 0;
    for m in s.members.values() {
        if m.rebalance_timeout_ms > max {
            max = m.rebalance_timeout_ms;
        }
    }
    if max <= 0 {
        DEFAULT_REBALANCE_TIMEOUT_MS
    } else {
        max
    }
}

fn select_protocol(
    members: &HashMap<String, GroupMember>,
    waiters: &[JoinWaiter],
) -> Option<String> {
    let first = waiters.first()?;
    let leader = members.get(&first.member_id)?;
    for p in &leader.protocols {
        let all_support = waiters[1..].iter().all(|w| {
            members
                .get(&w.member_id)
                .map(|m| m.protocols.iter().any(|mp| mp.name == p.name))
                .unwrap_or(false)
        });
        if all_support {
            return Some(p.name.clone());
        }
    }
    None
}

fn generate_member_id(client_id: &str) -> String {
    use std::fmt::Write;
    use std::sync::atomic::{AtomicU64, Ordering};
    // Cheap, deterministic-in-a-test sequence — a real port should
    // swap in `rand::rngs::OsRng`. The Manager's wrapper passes a
    // pre-seeded RNG in tests if needed; this fallback keeps the free
    // fn safe to call from sync code without dragging `rand` into
    // kaas-coordinator's dep set.
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let bytes = n.to_le_bytes();
    let mut out = String::with_capacity(client_id.len() + 1 + 16);
    out.push_str(client_id);
    out.push('-');
    for b in bytes {
        // write! into a String never fails — the `let _ =` is the
        // workspace clippy-compatible way to discard the Result.
        let _ = write!(out, "{b:02x}");
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn join_req(member_id: &str, client_id: &str, protocol: &str) -> JoinRequest {
        JoinRequest {
            member_id: member_id.to_owned(),
            group_instance_id: None,
            session_timeout_ms: 30_000,
            rebalance_timeout_ms: 30_000,
            protocol_type: "consumer".to_owned(),
            protocols: vec![ProtocolMetadata {
                name: protocol.to_owned(),
                metadata: Bytes::from_static(b"meta"),
            }],
            version: 9,
            client_id: client_id.to_owned(),
            client_host: "/127.0.0.1".to_owned(),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn single_member_rebalance_completes_via_initial_delay() {
        let g = Group::new("g1");
        let join = tokio::spawn({
            let g = g.clone();
            async move { g.join(join_req("", "consumer-1", "range")).await }
        });
        // Initial rebalance delay default is 3s; under `start_paused`
        // the test clock advances on demand.
        tokio::time::sleep(Duration::from_millis(INITIAL_REBALANCE_DELAY_MS + 100)).await;
        let outcome = join.await.unwrap();
        assert_eq!(outcome.error_code, error_codes::NONE);
        assert_eq!(outcome.generation_id, 1);
        assert_eq!(outcome.leader, outcome.member_id);
        assert_eq!(outcome.protocol_name, "range");
        assert_eq!(outcome.members.len(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn sync_returns_leader_supplied_assignment() {
        let g = Group::new("g1");
        let join_fut = tokio::spawn({
            let g = g.clone();
            async move { g.join(join_req("", "consumer-1", "range")).await }
        });
        tokio::time::sleep(Duration::from_millis(INITIAL_REBALANCE_DELAY_MS + 100)).await;
        let j = join_fut.await.unwrap();
        let sync = SyncRequest {
            member_id: j.member_id.clone(),
            generation_id: j.generation_id,
            group_instance_id: None,
            protocol_type: Some("consumer".to_owned()),
            protocol_name: Some("range".to_owned()),
            assignments: vec![SyncAssignment {
                member_id: j.member_id.clone(),
                assignment: Bytes::from_static(b"\x01\x02\x03"),
            }],
        };
        let out = g.sync(sync).await;
        assert_eq!(out.error_code, error_codes::NONE);
        assert_eq!(out.assignment.as_ref(), b"\x01\x02\x03");
        assert_eq!(g.state(), GroupState::Stable);
    }

    #[tokio::test(start_paused = true)]
    async fn heartbeat_unknown_member_for_empty_group() {
        let g = Group::new("g1");
        let code = g.heartbeat(HeartbeatRequest {
            member_id: "ghost".to_owned(),
            generation_id: 0,
            group_instance_id: None,
        });
        assert_eq!(code, error_codes::UNKNOWN_MEMBER_ID);
    }

    #[tokio::test(start_paused = true)]
    async fn leave_drops_state_back_to_empty() {
        let g = Group::new("g1");
        let join_fut = tokio::spawn({
            let g = g.clone();
            async move { g.join(join_req("", "consumer-1", "range")).await }
        });
        tokio::time::sleep(Duration::from_millis(INITIAL_REBALANCE_DELAY_MS + 100)).await;
        let j = join_fut.await.unwrap();
        let _ = g
            .sync(SyncRequest {
                member_id: j.member_id.clone(),
                generation_id: j.generation_id,
                assignments: vec![SyncAssignment {
                    member_id: j.member_id.clone(),
                    assignment: Bytes::new(),
                }],
                ..SyncRequest::default()
            })
            .await;
        let out = g.leave(std::slice::from_ref(&j.member_id));
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].1, error_codes::NONE);
        assert_eq!(g.state(), GroupState::Empty);
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_fires_pending_joiners_with_unknown_member() {
        let g = Group::new("g1");
        let g2 = g.clone();
        let join = tokio::spawn(async move { g2.join(join_req("", "consumer-1", "range")).await });
        // Trigger shutdown before the rebalance timer fires.
        tokio::time::sleep(Duration::from_millis(10)).await;
        g.shutdown();
        let outcome = join.await.unwrap();
        assert_eq!(outcome.error_code, error_codes::UNKNOWN_MEMBER_ID);
        assert_eq!(g.state(), GroupState::Dead);
    }
}
