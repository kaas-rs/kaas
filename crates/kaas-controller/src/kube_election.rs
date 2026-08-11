//! Kube-backed `LeaseElection` implementation.
//!
//! Wraps the `coordination.k8s.io/v1` `Lease` API in a hand-rolled
//! acquire + renew loop. We don't lean on `kube::runtime::lease`
//! because it doesn't expose `lease_transitions` cleanly, and we
//! need that exact value for the controller epoch fence that
//! `kaas-broker::Coordinator` consults.
//!
//! Algorithm:
//!
//! 1. `GET` the Lease. If missing, `CREATE` it with the current
//!    identity. Read its current `holder_identity` +
//!    `lease_transitions` + `renew_time`.
//! 2. If `holder_identity == self` and we're within
//!    `lease_duration`, refresh `renew_time` via
//!    server-side-apply and call ourselves elected with the
//!    current `lease_transitions` value.
//! 3. If `holder_identity` is unset OR the existing renew is
//!    stale (`renew_time + lease_duration < now`), patch the
//!    Lease via server-side-apply to take over: bump
//!    `lease_transitions += 1` if the holder changed, set
//!    `holder_identity = self_id`, stamp `renew_time = now`.
//! 4. Otherwise sleep `retry_period` and retry.
//!
//! Same election contract as earlier releases (same Lease object).

// Module-gating done at the `pub mod kube_election;` declaration
// in `lib.rs`; the duplicate `#![cfg(...)]` would trip clippy's
// duplicated-attribute lint.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::SecondsFormat;
use k8s_openapi::api::coordination::v1::Lease;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::MicroTime;
use kube::api::{Api, Patch, PatchParams, PostParams};
use kube::Client;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::election::LeaseElection;

/// Default lease duration. Matches Apache Kafka's controller-Lease
/// shape (15 s lease, 10 s renew, 2 s retry — gh #61).
pub const DEFAULT_LEASE_DURATION: Duration = Duration::from_secs(15);
pub const DEFAULT_RENEW_DEADLINE: Duration = Duration::from_secs(10);
pub const DEFAULT_RETRY_PERIOD: Duration = Duration::from_secs(2);

/// Annotation stamped on the Lease by [`KubeLeaseElection::release`]
/// recording who held it before `holderIdentity` was blanked. Lets a
/// subsequent acquire distinguish "same broker re-acquiring after its
/// own release" (no real handover — don't bump `leaseTransitions`)
/// from a genuine change of holder (gh #204). Self-cleaning: the
/// acquire apply omits it, so SSA prunes it once a real holder is set.
const LAST_HOLDER_ANNOTATION: &str = "kaas.rs/last-holder";

/// `leaseTransitions` is the controller epoch that fences
/// `assignment.json`; it must count *handovers*, not acquisitions.
/// Bump only when the previous holder — the live `holderIdentity` if
/// non-empty, else the [`LAST_HOLDER_ANNOTATION`] a release left
/// behind — is someone else or unknown (gh #204).
fn next_transitions(prev_holder: Option<&str>, identity: &str, current: i32) -> i32 {
    match prev_holder {
        Some(prev) if prev == identity => current,
        _ => current + 1,
    }
}

pub struct KubeLeaseElection {
    client: Client,
    namespace: String,
    lease_name: String,
    identity: String,
    lease_duration: Duration,
    renew_deadline: Duration,
    retry_period: Duration,
    /// Last `leaseTransitions` we observed while holding — used by
    /// [`Self::release`] so its apply preserves the counter. SSA
    /// prunes previously-owned fields missing from the body; a
    /// release that omitted `leaseTransitions` reset the controller
    /// epoch 119 → 1 live, letting stale on-disk assignment state
    /// outrank every subsequent controller.
    last_transitions: std::sync::atomic::AtomicI32,
}

impl std::fmt::Debug for KubeLeaseElection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KubeLeaseElection")
            .field("namespace", &self.namespace)
            .field("lease_name", &self.lease_name)
            .field("identity", &self.identity)
            .field("lease_duration", &self.lease_duration)
            .finish_non_exhaustive()
    }
}

impl KubeLeaseElection {
    pub fn new(
        client: Client,
        namespace: impl Into<String>,
        lease_name: impl Into<String>,
        identity: impl Into<String>,
    ) -> Arc<Self> {
        Arc::new(Self {
            client,
            namespace: namespace.into(),
            lease_name: lease_name.into(),
            identity: identity.into(),
            lease_duration: DEFAULT_LEASE_DURATION,
            renew_deadline: DEFAULT_RENEW_DEADLINE,
            retry_period: DEFAULT_RETRY_PERIOD,
            last_transitions: std::sync::atomic::AtomicI32::new(0),
        })
    }

    /// Override the lease cadence (test hook).
    pub fn with_timings(
        mut self: Arc<Self>,
        lease_duration: Duration,
        renew_deadline: Duration,
        retry_period: Duration,
    ) -> Arc<Self> {
        if let Some(inner) = Arc::get_mut(&mut self) {
            inner.lease_duration = lease_duration;
            inner.renew_deadline = renew_deadline;
            inner.retry_period = retry_period;
        }
        self
    }

    fn api(&self) -> Api<Lease> {
        Api::namespaced(self.client.clone(), &self.namespace)
    }

    /// Compute the canonical RFC3339 microtime stamp we write into
    /// `spec.renewTime`. Same shape Apache's controller-runtime
    /// uses.
    fn now_microtime() -> MicroTime {
        MicroTime(chrono::Utc::now())
    }

    fn now_rfc3339() -> String {
        chrono::Utc::now().to_rfc3339_opts(SecondsFormat::Micros, true)
    }

    /// Single acquire attempt. Returns `Ok(Some(epoch))` when the
    /// caller is elected, `Ok(None)` when another holder is still
    /// renewing, and `Err(...)` on transport / API errors.
    async fn try_acquire(&self) -> kube::Result<Option<i64>> {
        let api = self.api();
        let existing = api.get_opt(&self.lease_name).await?;
        let now = chrono::Utc::now();

        let (current_holder, current_transitions, renew_time, lease_seconds) = match &existing {
            None => (None, 0i32, None, None),
            Some(l) => {
                let spec = l.spec.clone().unwrap_or_default();
                (
                    spec.holder_identity,
                    spec.lease_transitions.unwrap_or(0),
                    spec.renew_time,
                    spec.lease_duration_seconds,
                )
            }
        };
        let last_holder_annotation = existing.as_ref().and_then(|l| {
            l.metadata
                .annotations
                .as_ref()
                .and_then(|a| a.get(LAST_HOLDER_ANNOTATION))
                .cloned()
        });

        let we_already_hold = current_holder.as_deref() == Some(self.identity.as_str());
        let lease_window = lease_seconds
            .map(|s| Duration::from_secs(u64::try_from(s.max(0)).unwrap_or(0)))
            .unwrap_or(self.lease_duration);
        let last_renew_age = renew_time
            .as_ref()
            .map(|MicroTime(t)| now.signed_duration_since(*t).to_std().unwrap_or_default());
        let lease_is_stale = last_renew_age.map(|age| age > lease_window).unwrap_or(true);
        // An empty holder means the previous controller released on
        // shutdown (client-go ReleaseOnCancel semantics) — free to
        // take without waiting out the lease window.
        let released = current_holder.as_deref().is_none_or(str::is_empty);

        if !we_already_hold && !lease_is_stale && !released {
            debug!(
                holder = current_holder.as_deref().unwrap_or("<none>"),
                "lease still held by another controller; retrying"
            );
            return Ok(None);
        }

        // The previous holder is the live one if set; a blank holder
        // means a release happened, and the release stamped who it
        // was in the annotation. Bumping on every re-acquire (the
        // pre-gh #204 behaviour) inflated the epoch by ~1000 with a
        // single broker and zero real handovers.
        let prev_holder = match current_holder.as_deref() {
            Some(h) if !h.is_empty() => Some(h),
            _ => last_holder_annotation.as_deref(),
        };
        let new_transitions = next_transitions(prev_holder, &self.identity, current_transitions);

        let duration_secs = i32::try_from(self.lease_duration.as_secs()).unwrap_or(i32::MAX);
        let patch = serde_json::json!({
            "apiVersion": "coordination.k8s.io/v1",
            "kind": "Lease",
            "metadata": { "name": self.lease_name, "namespace": self.namespace },
            "spec": {
                "holderIdentity": self.identity,
                "leaseDurationSeconds": duration_secs,
                "acquireTime": Self::now_rfc3339(),
                "renewTime": Self::now_rfc3339(),
                "leaseTransitions": new_transitions,
            }
        });

        if existing.is_some() {
            api.patch(
                &self.lease_name,
                &PatchParams::apply("kaas").force(),
                &Patch::Apply(&patch),
            )
            .await?;
        } else {
            let spec = k8s_openapi::api::coordination::v1::LeaseSpec {
                holder_identity: Some(self.identity.clone()),
                lease_duration_seconds: Some(duration_secs),
                acquire_time: Some(Self::now_microtime()),
                renew_time: Some(Self::now_microtime()),
                lease_transitions: Some(new_transitions),
                ..Default::default()
            };
            let lease = Lease {
                metadata: kube::api::ObjectMeta {
                    name: Some(self.lease_name.clone()),
                    namespace: Some(self.namespace.clone()),
                    ..Default::default()
                },
                spec: Some(spec),
            };
            api.create(&PostParams::default(), &lease).await?;
        }
        self.last_transitions
            .store(new_transitions, std::sync::atomic::Ordering::Relaxed);
        Ok(Some(i64::from(new_transitions)))
    }

    /// Refresh `renewTime` if we still hold the Lease. `Ok(true)` =
    /// renewed; `Ok(false)` = lost (holder changed or Lease gone);
    /// `Err` = transient API failure (caller decides when to give
    /// up via `renew_deadline`).
    async fn try_renew(&self) -> kube::Result<bool> {
        let api = self.api();
        let Some(lease) = api.get_opt(&self.lease_name).await? else {
            return Ok(false);
        };
        let spec = lease.spec.unwrap_or_default();
        if spec.holder_identity.as_deref() != Some(self.identity.as_str()) {
            return Ok(false);
        }
        let transitions = spec.lease_transitions.unwrap_or(0);
        let duration_secs = i32::try_from(self.lease_duration.as_secs()).unwrap_or(i32::MAX);
        let patch = serde_json::json!({
            "apiVersion": "coordination.k8s.io/v1",
            "kind": "Lease",
            "metadata": { "name": self.lease_name, "namespace": self.namespace },
            "spec": {
                "holderIdentity": self.identity,
                "leaseDurationSeconds": duration_secs,
                "renewTime": Self::now_rfc3339(),
                "leaseTransitions": transitions,
            }
        });
        api.patch(
            &self.lease_name,
            &PatchParams::apply("kaas").force(),
            &Patch::Apply(&patch),
        )
        .await?;
        self.last_transitions
            .store(transitions, std::sync::atomic::Ordering::Relaxed);
        Ok(true)
    }

    /// Best-effort release on shutdown: blank `holderIdentity` so
    /// the next candidate doesn't wait out the full lease window
    /// (client-go's ReleaseOnCancel). Errors are logged and
    /// swallowed — the lease going stale is the fallback.
    ///
    /// The apply body MUST carry `leaseTransitions` (and the lease
    /// duration): SSA prunes previously-owned fields that are
    /// missing, and wiping the transitions counter resets the
    /// cluster-wide controller epoch fence.
    async fn release(&self) {
        let transitions = self
            .last_transitions
            .load(std::sync::atomic::Ordering::Relaxed);
        let duration_secs = i32::try_from(self.lease_duration.as_secs()).unwrap_or(i32::MAX);
        let patch = serde_json::json!({
            "apiVersion": "coordination.k8s.io/v1",
            "kind": "Lease",
            "metadata": {
                "name": self.lease_name,
                "namespace": self.namespace,
                // Record who is releasing so the next acquire can
                // tell a self-re-acquire from a real handover and
                // leave `leaseTransitions` alone for the former
                // (gh #204). The acquire apply omits this key, so
                // SSA prunes it once a live holder is set again.
                "annotations": { LAST_HOLDER_ANNOTATION: self.identity },
            },
            "spec": {
                "holderIdentity": "",
                "leaseDurationSeconds": duration_secs,
                "renewTime": Self::now_rfc3339(),
                "leaseTransitions": transitions,
            }
        });
        if let Err(err) = self
            .api()
            .patch(
                &self.lease_name,
                &PatchParams::apply("kaas").force(),
                &Patch::Apply(&patch),
            )
            .await
        {
            warn!(%err, "lease election: release failed (lease will expire naturally)");
        }
    }

    /// Long-running election driver: acquire → `on_acquired(epoch,
    /// leader_token)` → renew every `retry_period` → on loss run
    /// `on_lost`, cancel `leader_token`, and re-enter candidacy.
    /// Returns only when `cancel` fires (best-effort releasing the
    /// Lease if held).
    ///
    /// Leader-election callbacks: `on_acquired` ≙
    /// start-of-leadership (spawn controller tasks bound to the
    /// token), `on_lost` ≙ `OnStoppedLeading` — invoked
    /// *synchronously before* any re-acquire, so state it flips
    /// (e.g. an is_controller gauge) can never trail a newer
    /// acquisition.
    pub async fn campaign<F, G>(
        self: Arc<Self>,
        cancel: CancellationToken,
        on_acquired: F,
        on_lost: G,
    ) where
        F: Fn(i64, CancellationToken) + Send + Sync,
        G: Fn() + Send + Sync,
    {
        loop {
            let epoch = tokio::select! {
                () = cancel.cancelled() => return,
                epoch = self.acquire() => epoch,
            };
            info!(epoch, identity = %self.identity, "controller lease acquired");
            let leader_token = cancel.child_token();
            on_acquired(epoch, leader_token.clone());

            // Leadership: renew until loss or shutdown. Transient
            // API errors are tolerated until `renew_deadline` of
            // continuous failure — mirrors client-go semantics.
            let mut first_err_at: Option<std::time::Instant> = None;
            loop {
                tokio::select! {
                    () = cancel.cancelled() => {
                        on_lost();
                        leader_token.cancel();
                        self.release().await;
                        return;
                    }
                    // The controller stack cancels its own token when
                    // it can't run (e.g. the initial assignment write
                    // failed). Release so another candidate takes over
                    // immediately instead of us renewing a leaderless
                    // lease.
                    () = leader_token.cancelled() => {
                        warn!(identity = %self.identity,
                              "controller stack ended; releasing lease and re-entering candidacy");
                        self.release().await;
                        break;
                    }
                    () = tokio::time::sleep(self.retry_period) => {}
                }
                // A single kube call must never block the loop past
                // the renew window — the API server on a loaded node
                // can stall for tens of seconds (observed p99 1.7 s,
                // long tail ≫ lease_duration during pod churn) and an
                // unbounded call here is how the lease gets stolen
                // out from under a healthy controller. Per-attempt
                // budget = renew_deadline (client-go semantics: one
                // slow-but-live call may use the whole window; a hung
                // one burns it once and we abdicate cleanly).
                let attempt_budget = self.renew_deadline.max(Duration::from_secs(3));
                match tokio::time::timeout(attempt_budget, self.try_renew()).await {
                    Ok(Ok(true)) => first_err_at = None,
                    Ok(Ok(false)) => {
                        warn!(identity = %self.identity, "controller lease lost to another holder");
                        break;
                    }
                    Ok(Err(err)) => {
                        let since = *first_err_at.get_or_insert_with(std::time::Instant::now);
                        if since.elapsed() > self.renew_deadline {
                            warn!(%err, "controller lease renew failed past renew_deadline; abdicating");
                            break;
                        }
                        warn!(%err, "controller lease renew failed; retrying");
                    }
                    Err(_elapsed) => {
                        let since = *first_err_at.get_or_insert_with(std::time::Instant::now);
                        if since.elapsed() > self.renew_deadline {
                            warn!(
                                "controller lease renew timed out past renew_deadline; abdicating"
                            );
                            break;
                        }
                        warn!(
                            budget_s = attempt_budget.as_secs(),
                            "controller lease renew attempt timed out; retrying"
                        );
                    }
                }
            }
            on_lost();
            leader_token.cancel();
        }
    }
}

#[async_trait]
impl LeaseElection for KubeLeaseElection {
    async fn acquire(&self) -> i64 {
        let started = std::time::Instant::now();
        loop {
            match self.try_acquire().await {
                Ok(Some(epoch)) => {
                    let m = kaas_observability::metrics::global();
                    m.controller_failovers.add(1, &[]);
                    m.controller_failover_duration
                        .record(started.elapsed().as_secs_f64(), &[]);
                    return epoch;
                }
                Ok(None) => {}
                Err(err) => {
                    warn!(%err, "lease election: try_acquire failed; retrying");
                }
            }
            tokio::time::sleep(self.retry_period).await;
        }
    }

    fn identity(&self) -> String {
        self.identity.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn now_rfc3339_is_microsecond_zulu() {
        let s = KubeLeaseElection::now_rfc3339();
        assert!(s.ends_with('Z'), "must be UTC");
        // YYYY-MM-DDTHH:MM:SS.ffffffZ → 27 chars.
        assert_eq!(s.len(), 27, "got {s:?}");
    }

    #[test]
    fn a_self_reacquire_after_release_keeps_the_epoch() {
        // gh #204: release blanks holderIdentity but the annotation
        // remembers us; taking the lease back is not a transition.
        assert_eq!(next_transitions(Some("kaas-1"), "kaas-1", 42), 42);
    }

    #[test]
    fn a_real_handover_bumps_the_epoch() {
        assert_eq!(next_transitions(Some("kaas-0"), "kaas-1", 42), 43);
    }

    #[test]
    fn an_unknown_previous_holder_bumps_conservatively() {
        // Fresh lease, or a pre-annotation release: no way to prove
        // it was us, so the fence must move forward.
        assert_eq!(next_transitions(None, "kaas-1", 0), 1);
    }

    #[test]
    fn a_renew_while_holding_keeps_the_epoch() {
        // try_acquire while we are still the live holder.
        assert_eq!(next_transitions(Some("kaas-2"), "kaas-2", 7), 7);
    }
}
