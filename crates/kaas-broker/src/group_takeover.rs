//! `GroupTakeoverDriver` — consumer-group analogue of
//! [`TakeoverDriver`].
//!
//! Watches
//! assignment changes and tells `coordinator::Manager` to drop
//! in-memory state for groups no longer assigned here.
//!
//! One pass, over everything resident in memory: a group this broker
//! no longer coordinates under `next` is relinquished. That covers
//! both the prev → next handover and the gh #89 stale-`--list` leak
//! (a stray in-memory entry that landed during a brief "I own this"
//! window the broker has since overwritten), because a group can only
//! be relinquished if it is resident in the first place.
//!
//! **Ownership is asked the same way the rest of the broker asks it**
//! (gh #248): explicit `assignment.json.consumerGroups[]` entry first,
//! `group_hash` fallthrough otherwise — the rule in
//! `Coordinator::owns_group`, which is what `Manager::is_coordinator`
//! and every group handler consult. Reading the explicit list alone
//! makes this driver disagree with the broker it is driving: a group
//! owned via the hash tier — every brand-new group, and any group
//! whose explicit entry got retired — looks unowned here and is
//! relinquished on *every* assignment change, however unrelated.
//! Relinquishing drops the member list, so the group's next heartbeat
//! is answered `UNKNOWN_MEMBER_ID` and the whole group rebuilds,
//! while ownership never actually moved.
//!
//! v1 does not migrate group state across brokers — the new
//! coordinator's first JoinGroup creates the group via
//! `Manager::get_or_create`, which lazily loads persisted offsets.
//! Acceptable cost: one rebalance round-trip per coordinator
//! transition.
//!
//! [`TakeoverDriver`]: crate::takeover::TakeoverDriver

use std::collections::HashSet;
use std::sync::Arc;

use kaas_coordinator::Manager;

use crate::assignment::Assignment;

pub struct GroupTakeoverDriver {
    mgr: Arc<Manager>,
    broker_id: String,
}

impl std::fmt::Debug for GroupTakeoverDriver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GroupTakeoverDriver")
            .field("broker_id", &self.broker_id)
            .finish_non_exhaustive()
    }
}

impl GroupTakeoverDriver {
    pub fn new(mgr: Arc<Manager>, broker_id: impl Into<String>) -> Arc<Self> {
        Arc::new(Self {
            mgr,
            broker_id: broker_id.into(),
        })
    }

    /// Build the [`AssignmentChangeHandler`] closure for this
    /// driver. Register it with `coordinator.on_assignment_change(...)`
    /// at boot.
    ///
    /// [`AssignmentChangeHandler`]: crate::assignment::AssignmentChangeHandler
    pub fn as_handler(self: &Arc<Self>) -> crate::assignment::AssignmentChangeHandler {
        let me = self.clone();
        Arc::new(move |prev, next| me.on_change(prev, next))
    }

    pub fn on_change(&self, prev: Option<&Assignment>, next: &Assignment) {
        let mut seen: HashSet<String> = HashSet::new();
        for group_id in self.mgr.resident_groups() {
            if !seen.insert(group_id.clone()) {
                continue;
            }
            if owns_group(next, &group_id, &self.broker_id) {
                continue;
            }
            // gh #248: a coordinator move was only ever reconstructible
            // from the *client's* log — nothing on the broker said the
            // group had left. Log both directions at info.
            tracing::info!(
                group_id = %group_id,
                from = %self.broker_id,
                to = coordinator_of(next, &group_id).as_deref().unwrap_or("<none>"),
                assignment_version = next.assignment_version,
                "consumer-group coordinator moved away; relinquishing in-memory state"
            );
            self.mgr.relinquish_group(&group_id);
        }

        for g in &next.consumer_groups {
            if g.broker != self.broker_id {
                continue;
            }
            let was = prev.and_then(|p| coordinator_of(p, &g.group_id));
            if was.as_deref() == Some(self.broker_id.as_str()) {
                continue;
            }
            tracing::info!(
                group_id = %g.group_id,
                from = was.as_deref().unwrap_or("<none>"),
                to = %self.broker_id,
                assignment_version = next.assignment_version,
                "consumer-group coordinator moved here"
            );
        }
    }
}

/// Does `broker_id` coordinate `group_id` under `a`? Mirrors
/// `Coordinator::owns_group` — explicit entry first, `group_hash`
/// fallthrough otherwise. The two must not drift apart; see the
/// module docs.
fn owns_group(a: &Assignment, group_id: &str, broker_id: &str) -> bool {
    coordinator_of(a, group_id).is_some_and(|b| b == broker_id)
}

/// The broker coordinating `group_id` under `a`, or `None` when no
/// broker is alive to take it.
fn coordinator_of(a: &Assignment, group_id: &str) -> Option<String> {
    for g in &a.consumer_groups {
        if g.group_id == group_id {
            return Some(g.broker.clone());
        }
    }
    let (brokers, alive) = a.broker_sets();
    crate::group_hash::pick_group_coordinator(group_id, &brokers, &alive)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use kaas_coordinator::{
        BrokerEndpoint, BrokerLookup, FnLookup, GroupAssignmentSource, JoinRequest, Manager,
        OffsetStore, ProtocolMetadata,
    };

    use crate::assignment::{BrokerAssignment, BrokerHealth, ConsumerGroupAssignment};

    /// Assignment over `kaas-0..2`, all alive, with whatever explicit
    /// group entries the test wants.
    fn assignment(version: i64, groups: &[(&str, &str)]) -> Assignment {
        Assignment {
            controller_epoch: 1,
            assignment_version: version,
            generated_at: "2026-08-09T00:00:00Z".to_owned(),
            controller: "kaas-0".to_owned(),
            brokers: (0..3)
                .map(|i| BrokerAssignment {
                    id: format!("kaas-{i}"),
                    health: BrokerHealth::Alive,
                    last_seen: "2026-08-09T00:00:00Z".to_owned(),
                })
                .collect(),
            partitions: Vec::new(),
            consumer_groups: groups
                .iter()
                .map(|(g, b)| ConsumerGroupAssignment {
                    group_id: (*g).to_owned(),
                    broker: (*b).to_owned(),
                    epoch: 1,
                })
                .collect(),
        }
    }

    /// A group id the hash tier resolves to `broker` — i.e. one that
    /// needs no explicit `consumerGroups[]` entry to be ours.
    fn group_hashing_to(broker: &str, a: &Assignment) -> String {
        let (brokers, alive) = a.broker_sets();
        (0..500)
            .map(|i| format!("g{i}"))
            .find(|g| {
                crate::group_hash::pick_group_coordinator(g, &brokers, &alive).as_deref()
                    == Some(broker)
            })
            .expect("some group id hashes to this broker")
    }

    /// Group source that answers from an `Assignment`, exactly like
    /// the production `Coordinator` does.
    #[derive(Debug)]
    struct AssignmentSource {
        assignment: Assignment,
        self_id: String,
    }
    impl GroupAssignmentSource for AssignmentSource {
        fn owns_group(&self, group_id: &str) -> bool {
            super::owns_group(&self.assignment, group_id, &self.self_id)
        }
        fn group_coordinator(&self, group_id: &str) -> Option<String> {
            coordinator_of(&self.assignment, group_id)
        }
    }

    fn manager(tmp: &tempfile::TempDir, source: Arc<dyn GroupAssignmentSource>) -> Arc<Manager> {
        let lookup: Arc<dyn BrokerLookup> = Arc::new(FnLookup::new(|id: &str| {
            Some(BrokerEndpoint {
                node_id: 0,
                host: format!("{id}.local"),
                port: 9092,
            })
        }));
        Manager::new(
            "kaas-0",
            Arc::new(OffsetStore::new(tmp.path())),
            lookup,
            source,
        )
    }

    async fn materialise(mgr: &Manager, group_id: &str) {
        mgr.join_group(
            group_id,
            JoinRequest {
                member_id: String::new(),
                group_instance_id: None,
                session_timeout_ms: 10_000,
                rebalance_timeout_ms: 10_000,
                protocol_type: "consumer".to_owned(),
                protocols: vec![ProtocolMetadata {
                    name: "range".to_owned(),
                    metadata: Default::default(),
                }],
                version: 5,
                client_id: "c1".to_owned(),
                client_host: "127.0.0.1".to_owned(),
            },
        )
        .await;
    }

    #[tokio::test]
    async fn a_hash_owned_group_survives_an_unrelated_assignment_change() {
        // gh #248: the driver used to read the explicit
        // `consumerGroups[]` list only. A group owned via the hash
        // fallthrough — every brand-new group, and any whose entry was
        // retired — looked unowned and was relinquished on every
        // recompute, dropping its members mid-flight for a coordinator
        // move that never happened.
        let tmp = tempfile::tempdir().unwrap();
        let a1 = assignment(1, &[]);
        let mine = group_hashing_to("kaas-0", &a1);

        let mgr = manager(
            &tmp,
            Arc::new(AssignmentSource {
                assignment: a1.clone(),
                self_id: "kaas-0".to_owned(),
            }),
        );
        materialise(&mgr, &mine).await;
        assert_eq!(mgr.resident_groups(), vec![mine.clone()]);

        // An unrelated topic change: same brokers, same groups, new
        // version. Nothing about `mine`'s coordination changed.
        let driver = GroupTakeoverDriver::new(mgr.clone(), "kaas-0");
        driver.on_change(Some(&a1), &assignment(2, &[]));

        assert_eq!(
            mgr.resident_groups(),
            vec![mine],
            "hash-owned group was relinquished by an unrelated recompute"
        );
    }

    #[tokio::test]
    async fn a_group_reassigned_elsewhere_is_relinquished() {
        let tmp = tempfile::tempdir().unwrap();
        let a1 = assignment(1, &[("g", "kaas-0")]);
        let a2 = assignment(2, &[("g", "kaas-1")]);
        let mgr = manager(
            &tmp,
            Arc::new(AssignmentSource {
                assignment: a1.clone(),
                self_id: "kaas-0".to_owned(),
            }),
        );
        materialise(&mgr, "g").await;

        // The controller hands the group to kaas-1.
        mgr.set_group_assignment_source(Arc::new(AssignmentSource {
            assignment: a2.clone(),
            self_id: "kaas-0".to_owned(),
        }));

        // The manager's own source now says kaas-1 owns it, so
        // `local_groups()` filters it out — the pre-gh #248 sweep read
        // that list and so could never see the group it needed to
        // drop. `resident_groups()` is what makes the sweep complete.
        assert!(mgr.local_groups().is_empty());
        assert_eq!(mgr.resident_groups(), vec!["g".to_owned()]);

        let driver = GroupTakeoverDriver::new(mgr.clone(), "kaas-0");
        driver.on_change(Some(&a1), &a2);

        assert!(
            mgr.resident_groups().is_empty(),
            "a group reassigned to another broker must be dropped"
        );
    }
}
