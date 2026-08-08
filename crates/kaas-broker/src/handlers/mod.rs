//! Broker-side API handler implementations.
//!
//! One file per API; each impl satisfies the
//! [`kaas_protocol::Handler`] trait. The host (`bins/kaas/main.rs`)
//! registers them on a [`kaas_protocol::Dispatcher`].

pub mod acls;
pub mod add_offsets_to_txn;
pub mod add_partitions_to_txn;
pub mod alter_client_quotas;
pub mod alter_replica_log_dirs;
pub mod api_versions;
pub mod create_partitions;
pub mod create_topics;
pub mod delete_groups;
pub mod delete_records;
pub mod delete_topics;
pub mod describe_client_quotas;
pub mod describe_cluster;
pub mod describe_configs;
pub mod describe_groups;
pub mod describe_log_dirs;
pub mod end_txn;
pub mod fetch;
pub mod find_coordinator;
pub mod heartbeat;
pub mod incremental_alter_configs;
pub mod init_producer_id;
pub mod join_group;
pub mod leave_group;
pub mod list_groups;
pub mod list_offsets;
pub mod metadata;
pub mod offset_commit;
pub mod offset_delete;
pub mod offset_fetch;
pub mod produce;
pub mod sasl;
pub mod sync_group;
pub mod txn_offset_commit;
pub mod write_txn_markers;

pub use acls::{CreateAclsHandler, DeleteAclsHandler, DescribeAclsHandler};
pub use add_offsets_to_txn::AddOffsetsToTxnHandler;
pub use add_partitions_to_txn::AddPartitionsToTxnHandler;
pub use alter_client_quotas::AlterClientQuotasHandler;
pub use alter_replica_log_dirs::{migrate_partition, AlterReplicaLogDirsHandler};
pub use api_versions::ApiVersionsHandler;
pub use create_partitions::CreatePartitionsHandler;
pub use create_topics::CreateTopicsHandler;
pub use delete_groups::DeleteGroupsHandler;
pub use delete_records::DeleteRecordsHandler;
pub use delete_topics::DeleteTopicsHandler;
pub use describe_client_quotas::DescribeClientQuotasHandler;
pub use describe_cluster::DescribeClusterHandler;
pub use describe_configs::DescribeConfigsHandler;
pub use describe_groups::DescribeGroupsHandler;
pub use describe_log_dirs::DescribeLogDirsHandler;
pub use end_txn::EndTxnHandler;
pub use fetch::FetchHandler;
pub use find_coordinator::FindCoordinatorHandler;
pub use heartbeat::HeartbeatHandler;
pub use incremental_alter_configs::IncrementalAlterConfigsHandler;
pub use init_producer_id::InitProducerIdHandler;
pub use join_group::JoinGroupHandler;
pub use leave_group::LeaveGroupHandler;
pub use list_groups::ListGroupsHandler;
pub use list_offsets::ListOffsetsHandler;
pub use metadata::MetadataHandler;
pub use offset_commit::OffsetCommitHandler;
pub use offset_delete::OffsetDeleteHandler;
pub use offset_fetch::OffsetFetchHandler;
pub use produce::ProduceHandler;
pub use sasl::{SaslAuthenticateHandler, SaslHandshakeHandler};
pub use sync_group::SyncGroupHandler;
pub use txn_offset_commit::TxnOffsetCommitHandler;
pub use write_txn_markers::WriteTxnMarkersHandler;
