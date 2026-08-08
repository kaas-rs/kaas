// Phase 0 smoke test: prove tonic-build actually ran and produced reachable types.

use kaas_broker::heartbeatpb::{partition_status::State, BrokerStatus, PartitionStatus};

#[test]
fn broker_status_is_constructible() {
    let status = BrokerStatus {
        broker_id: "kaas-0".to_string(),
        timestamp_ms: 1,
        last_seen_assignment_version: 0,
        partitions: vec![PartitionStatus {
            topic: "t".to_string(),
            partition: 0,
            epoch: 1,
            state: State::Ready.into(),
            high_watermark: 0,
        }],
        active_groups: vec![],
        healthy: true,
        draining: false,
    };
    assert_eq!(status.broker_id, "kaas-0");
    assert_eq!(status.partitions.len(), 1);
}
