# API support matrix

<!-- GENERATED FILE — do not edit. Regenerate with `cargo xtask gen-api-matrix`;
`cargo xtask check-docs-drift` (CI) fails when this file drifts from
crates/kaas-codec/src/api/registry.rs. -->

kaas registers **38 Kafka API keys**. This table is generated from the
`ApiSpec` registry (`crates/kaas-codec/src/api/registry.rs`) — the same table
that builds the ApiVersions response — so the version ranges below are the
wire truth, not documentation aspiration. "Flexible" is the first version
using KIP-482 flexible encoding (see [Wire protocol & framing](wire-protocol.md)).

| Key | API | Versions | Flexible | KIPs | Reference |
|--:|---|---|---|---|---|
| 0 | Produce | v3–v9 | v9+ | [KIP-98](kip/kip-98.md) · [KIP-360](kip/kip-360.md) · [KIP-32](kip/kip-32.md) | [Produce](api/produce-fetch.md#produce) |
| 1 | Fetch | v4–v12 | v12+ | [KIP-98](kip/kip-98.md) · [KIP-227](non-goals.md) | [Fetch](api/produce-fetch.md#fetch) |
| 2 | ListOffsets | v1–v7 | v6+ | [KIP-32](kip/kip-32.md) | [ListOffsets](api/produce-fetch.md#listoffsets) |
| 3 | Metadata | v1–v10 | v9+ | [KIP-516](kip/kip-516.md) | [Metadata](api/produce-fetch.md#metadata) |
| 8 | OffsetCommit | v2–v8 | v8+ | — | [OffsetCommit](api/consumer-groups.md#offsetcommit) |
| 9 | OffsetFetch | v1–v8 | v6+ | [KIP-447](kip/kip-447.md) | [OffsetFetch](api/consumer-groups.md#offsetfetch) |
| 10 | FindCoordinator | v0–v4 | v3+ | — | [FindCoordinator](api/consumer-groups.md#findcoordinator) |
| 11 | JoinGroup | v2–v9 | v6+ | [KIP-345](kip/kip-345.md) · [KIP-394](kip/kip-394.md) · [KIP-800](kip/kip-800.md) | [JoinGroup](api/consumer-groups.md#joingroup) |
| 12 | Heartbeat | v0–v4 | v4+ | [KIP-345](kip/kip-345.md) | [Heartbeat](api/consumer-groups.md#heartbeat) |
| 13 | LeaveGroup | v0–v5 | v4+ | [KIP-345](kip/kip-345.md) · [KIP-800](kip/kip-800.md) | [LeaveGroup](api/consumer-groups.md#leavegroup) |
| 14 | SyncGroup | v0–v5 | v4+ | [KIP-345](kip/kip-345.md) | [SyncGroup](api/consumer-groups.md#syncgroup) |
| 15 | DescribeGroups | v0–v5 | v5+ | — | [DescribeGroups](api/consumer-groups.md#describegroups) |
| 16 | ListGroups | v0–v4 | v3+ | — | [ListGroups](api/consumer-groups.md#listgroups) |
| 17 | SaslHandshake | v0–v1 | — | — | [SaslHandshake](api/auth.md#saslhandshake) |
| 18 | ApiVersions | v0–v4 | v3+ | [KIP-482](kip/kip-482.md) | [ApiVersions](api/cluster-misc.md#apiversions) |
| 19 | CreateTopics | v0–v7 | v5+ | [KIP-516](kip/kip-516.md) | [CreateTopics](api/topics-configs.md#createtopics) |
| 20 | DeleteTopics | v0–v5 | v4+ | — | [DeleteTopics](api/topics-configs.md#deletetopics) |
| 21 | DeleteRecords | v0–v2 | v2+ | [KIP-107](kip/kip-107.md) | [DeleteRecords](api/topics-configs.md#deleterecords) |
| 22 | InitProducerId | v0–v4 | v2+ | [KIP-98](kip/kip-98.md) · [KIP-360](kip/kip-360.md) | [InitProducerId](api/transactions.md#initproducerid) |
| 24 | AddPartitionsToTxn | v0–v3 | v3+ | [KIP-98](kip/kip-98.md) | [AddPartitionsToTxn](api/transactions.md#addpartitionstotxn) |
| 25 | AddOffsetsToTxn | v0–v3 | v3+ | [KIP-98](kip/kip-98.md) · [KIP-447](kip/kip-447.md) | [AddOffsetsToTxn](api/transactions.md#addoffsetstotxn) |
| 26 | EndTxn | v0–v3 | v3+ | [KIP-98](kip/kip-98.md) | [EndTxn](api/transactions.md#endtxn) |
| 27 | WriteTxnMarkers | v0–v1 | v1+ | [KIP-98](kip/kip-98.md) | [WriteTxnMarkers](api/transactions.md#writetxnmarkers) |
| 28 | TxnOffsetCommit | v0–v3 | v3+ | [KIP-447](kip/kip-447.md) | [TxnOffsetCommit](api/transactions.md#txnoffsetcommit) |
| 29 | DescribeAcls | v0–v3 | v2+ | [KIP-290](kip/kip-290.md) | [DescribeAcls](api/acls-quotas.md#describeacls) |
| 30 | CreateAcls | v0–v3 | v2+ | [KIP-290](kip/kip-290.md) | [CreateAcls](api/acls-quotas.md#createacls) |
| 31 | DeleteAcls | v0–v3 | v2+ | [KIP-290](kip/kip-290.md) | [DeleteAcls](api/acls-quotas.md#deleteacls) |
| 32 | DescribeConfigs | v0–v4 | v4+ | — | [DescribeConfigs](api/topics-configs.md#describeconfigs) |
| 34 | AlterReplicaLogDirs | v0–v2 | v2+ | — | [AlterReplicaLogDirs](api/cluster-misc.md#alterreplicalogdirs) |
| 35 | DescribeLogDirs | v0–v4 | v2+ | — | [DescribeLogDirs](api/cluster-misc.md#describelogdirs) |
| 36 | SaslAuthenticate | v0–v2 | v2+ | — | [SaslAuthenticate](api/auth.md#saslauthenticate) |
| 37 | CreatePartitions | v0–v3 | v2+ | [KIP-195](kip/kip-195.md) | [CreatePartitions](api/topics-configs.md#createpartitions) |
| 42 | DeleteGroups | v0–v2 | v2+ | — | [DeleteGroups](api/consumer-groups.md#deletegroups) |
| 44 | IncrementalAlterConfigs | v0–v1 | v1+ | [KIP-339](kip/kip-339.md) | [IncrementalAlterConfigs](api/topics-configs.md#incrementalalterconfigs) |
| 47 | OffsetDelete | v0–v0 | — | — | [OffsetDelete](api/consumer-groups.md#offsetdelete) |
| 48 | DescribeClientQuotas | v0–v1 | v1+ | [KIP-546](kip/kip-546.md) | [DescribeClientQuotas](api/acls-quotas.md#describeclientquotas) |
| 49 | AlterClientQuotas | v0–v1 | v1+ | [KIP-546](kip/kip-546.md) | [AlterClientQuotas](api/acls-quotas.md#alterclientquotas) |
| 60 | DescribeCluster | v0–v1 | v0+ | [KIP-700](kip/kip-700.md) | [DescribeCluster](api/cluster-misc.md#describecluster) |

## Apache 3.7 keys kaas does not serve

Clients discover the served surface via ApiVersions, so an absent key is a
clean "unsupported", not an error path. Each absence is either a tracked
follow-up or a documented [non-goal](non-goals.md):

| Key | API | Status |
|--:|---|---|
| 23 | OffsetForLeaderEpoch | [KIP-101](kip/kip-101.md) partial — storage-side lookup returns the `(-1,-1)` sentinel; key unregistered. Open follow-up. |
| 33 | AlterConfigs (legacy) | Superseded by [IncrementalAlterConfigs](api/topics-configs.md#incrementalalterconfigs) (key 44) but still served by Apache 3.7. Open follow-up. |
| 50 | DescribeUserScramCredentials | [KIP-554](kip/kip-554.md) partial — credential rotation is operator-side only; no codec module, no dispatch. Open follow-up. |
| 51 | AlterUserScramCredentials | [KIP-554](kip/kip-554.md) partial — same as key 50. Open follow-up. |

Inter-broker/KRaft keys (LeaderAndIsr, StopReplica, UpdateMetadata,
ControlledShutdown, the quorum/Envelope family), delegation-token keys, and
tiered-storage-only surfaces are deliberately absent — see
[Non-goals](non-goals.md).
