# Topic & config admin APIs

Per-API reference — see the [API support matrix](../api-matrix.md) for the generated version table.

The whole admin surface on this page is **CR-mediated**: kaas never mutates
topic state directly off a wire request. Writes become creates/patches/deletes
of `KafkaTopic` custom resources,
the operator reconciles the CR into on-disk state, and the broker observes the
result through its topic watcher — see
[Kubernetes integration](../../architecture/kubernetes.md). In dev mode
(`MY_POD_NAME` unset, no kube client) the CR writer is a stub that refuses
every write, so the mutating APIs answer `CLUSTER_AUTHORIZATION_FAILED` (31)
with the message `broker is not running in cluster mode`.

## CreateTopics

Creates topics — the broker side of `kafka-topics.sh --create` and
`AdminClient.createTopics()`.

**Versions**: v0–v7 (flexible from v5).

**Handling**: per requested topic, the handler authorizes `Create` on the
topic resource, then POSTs a fresh `KafkaTopic` CR. The operator reconciles it
into partition directories on the shared volume; the broker picks the topic up
via its topic watcher and serves it on subsequent requests — creation is
therefore **asynchronous** (a success response means the CR was accepted, not
that partition dirs exist yet). A non-positive `num_partitions` (the
AdminClient's "server default" convention) maps to 1, mirroring Apache's
`num.partitions=1` default; the same rule applies to `replication_factor`.
Kafka topic names that aren't valid RFC 1123 subdomains (Kafka Streams
internals, dotted names) get a deterministic synthetic CR name
`kaas-topic-<16 hex>` with the literal name stashed in `spec.topicName`.
`validate_only` (v1+) runs the authorization and writer checks, then returns
the would-be response without minting the CR. Error mapping: authorization
denial → `TOPIC_AUTHORIZATION_FAILED` (29), existing CR →
`TOPIC_ALREADY_EXISTS` (36), missing writer or Kubernetes RBAC denial →
`CLUSTER_AUTHORIZATION_FAILED` (31), other kube errors →
`UNKNOWN_SERVER_ERROR` (-1).

**Deviations from Apache 3.7**:

- Config overrides on the request (`retention.ms=...` at create time) are
  decoded for protocol fidelity but **ignored** — the CR is created with a
  default `spec.config`. Set configs afterwards via
  [IncrementalAlterConfigs](#incrementalalterconfigs); threading them through
  the initial POST is tracked follow-up.
- The v7+ response `topic_id` ([KIP-516](../kip/kip-516.md)) is always the
  all-zero UUID: the real TopicID is minted by the operator on first
  reconcile, after the response has gone out.
- `replication_factor` is accepted and echoed but has no effect — kaas is
  single-writer-per-partition by design (see [Non-goals](../non-goals.md)).
- `validate_only` does not check for an existing topic; it reports success
  even when a real create would return `TOPIC_ALREADY_EXISTS`.

**Source**: `crates/kaas-broker/src/handlers/create_topics.rs`,
`crates/kaas-broker/src/topic_cr_writer.rs`,
`crates/kaas-codec/src/api/create_topics.rs`.

**Verified by**: `scripts/kafka-topics.sh` (create/list/describe scenarios);
codec round-trip tests in `crates/kaas-codec/src/api/create_topics.rs`
(including `v7_carries_topic_id`); CR-name mapping tests in
`crates/kaas-broker/src/topic_cr_writer.rs`.

## DeleteTopics

Deletes topics by name — `kafka-topics.sh --delete`,
`AdminClient.deleteTopics()`.

**Versions**: v0–v5 (flexible from v4).

**Handling**: per topic, the handler deletes the `KafkaTopic` CR, then drops
the topic from the in-memory registry. The operator's reconcile tears down the
partition directories; before that lands, the topic watcher fires the
topic-deleted event on every broker the moment `deletionTimestamp` appears, so
the leader closes its open log/index file handles first — otherwise NFS
silly-renames the open files and the operator's directory delete wedges (see
[File-handle ownership](../../architecture/file-handles.md)). A missing CR
answers `UNKNOWN_TOPIC_OR_PARTITION` (3); other writer errors are reported as
`INVALID_REQUEST` (42) with a message. In dev mode only the registry removal
runs — on-disk (in-memory-engine) data is left alone.

**Deviations from Apache 3.7**:

- Authorization: `Delete` on the topic per entry, as in Apache — denial
  answers `TOPIC_AUTHORIZATION_FAILED` (29) and skips the CR delete.
- Deletion is asynchronous: the wire response confirms the CR delete, while
  directory teardown follows on the operator's reconcile.

**Source**: `crates/kaas-broker/src/handlers/delete_topics.rs`,
`crates/kaas-broker/src/topic_cr_writer.rs`.

**Verified by**: `scripts/kafka-topics.sh` (scenario 5, delete-and-confirm).

## DeleteRecords

Advances a partition's log start offset ([KIP-107](../kip/kip-107.md)) —
`kafka-delete-records.sh`, Kafbat-UI's "purge messages".

**Versions**: v0–v2 (flexible from v2).

**Handling**: this is a storage-path API, not a CR write. Per partition the
handler applies the same ownership gate Produce uses — with a cluster
coordinator wired, partitions this broker doesn't lead answer
`NOT_LEADER_OR_FOLLOWER` (6). The storage engine then advances `logStart` to
the target offset (`-1` = purge to the high watermark; a target past the HWM
is `OFFSET_OUT_OF_RANGE` (1)) and returns the new low watermark. Records below
`logStart` become invisible to Fetch immediately, and closed segments that
fall entirely below it are unlinked from disk on the spot — safe on NFS
because only the leader holds open handles.

**Deviations from Apache 3.7**:

- The **active segment is not rolled or reclaimed** by DeleteRecords, and a
  closed segment only partially covered by the purge is kept whole. Visibility
  moves immediately; the covering bytes are reclaimed later by segment roll
  and retention. Apache behaves similarly for partial segments but kaas holds
  the active segment even when the purge covers the entire log.
- Authorization: `Delete` on the topic, as in Apache — denial answers
  `TOPIC_AUTHORIZATION_FAILED` (29) per partition and nothing is purged.

**Source**: `crates/kaas-broker/src/handlers/delete_records.rs`,
`crates/kaas-storage/src/partition.rs` (`delete_records`),
`crates/kaas-storage/src/disk.rs`.

**Verified by**: `scripts/kafka-delete-records.sh` (produce 10, purge to 7,
assert earliest = 7); `delete_records_*` unit tests in
`crates/kaas-storage/src/partition.rs` and `crates/kaas-storage/src/memory.rs`.

## DescribeConfigs

Reads topic and broker configuration — `kafka-configs.sh --describe` and every
admin UI's config pane.

**Versions**: v0–v4 (flexible from v4).

**Handling**: two resource types are served. **TOPIC**: authorize
`DescribeConfigs` on the topic (denial → 29), require the topic in the
registry (miss → `UNKNOWN_TOPIC_OR_PARTITION` (3)), then answer a static
Apache-3.7-compatible defaults table of the six config keys kaas actually
honours: `retention.ms`, `retention.bytes`, `segment.bytes`, `cleanup.policy`,
`min.compaction.lag.ms`, `delete.retention.ms`. v1+ attaches one
`DEFAULT_CONFIG` synonym per entry (mirroring Apache), v3+ adds one-line
documentation strings, and the request's `configuration_keys` filter is
honoured. **BROKER**: answers a small fixed read-only table (`broker.id` plus
static defaults) so `kafka-configs.sh --entity-type brokers` and Kafbat-UI's
broker page work. Everything else (`BROKER_LOGGER` included) gets a
per-resource `UNSUPPORTED_VERSION` (35).

**Deviations from Apache 3.7**:

- **Per-topic overrides are not surfaced.** Even when a topic's
  `KafkaTopic.spec.config` overrides retention, the response reports the
  cluster default with `is_default = true` / source `DEFAULT_CONFIG`. The
  override *is* enforced by the storage engine's cleaner — it just isn't
  echoed here yet. `kafka-configs.sh --describe` after `--alter` will not show
  the change.
- Only six topic keys are reported, versus Apache's several dozen; tools that
  iterate the full key set see a short list.
- The broker table reports static `kafka.version = 3.6.0` /
  `inter.broker.protocol.version = 3.6` strings (predating the 3.7 parity
  target).
- `BROKER_LOGGER` is unsupported and answers `UNSUPPORTED_VERSION` (35),
  where Apache serves log4j levels.

**Source**: `crates/kaas-broker/src/handlers/describe_configs.rs`,
`crates/kaas-broker/src/topic_config_defaults.rs`.

**Verified by**: `scripts/kafka-configs.sh` (broker describe, topic describe,
`--describe --all`, per-broker-id describe).

## CreatePartitions

Grows a topic's partition count ([KIP-195](../kip/kip-195.md)) —
`kafka-topics.sh --alter --partitions N`.

**Versions**: v0–v3 (flexible from v2).

**Handling**: authorize `Alter` on the topic (denial → 29), then merge-patch
`KafkaTopic.spec.partitions` to the new count. The writer reads the CR first
and refuses a decrease client-side with `INVALID_PARTITIONS` (37) — the
operator's reconciler enforces the same guard as backstop. A missing CR is
`UNKNOWN_TOPIC_OR_PARTITION` (3); dev mode / RBAC denial is
`CLUSTER_AUTHORIZATION_FAILED` (31). The operator creates the new partition
directories on reconcile and the broker serves them after its watcher fires —
expansion is asynchronous, same as topic creation. `validate_only` (v1+)
short-circuits before the patch.

**Deviations from Apache 3.7**:

- A request for the **same** partition count succeeds as a no-op; Apache
  returns `INVALID_PARTITIONS` when the requested count doesn't exceed the
  current one. Only a strict decrease is refused.
- The request's manual `assignments` (replica placement per new partition) are
  ignored — there are no replicas to place (see
  [Non-goals](../non-goals.md)); partition-to-broker placement is the
  controller's job.

**Source**: `crates/kaas-broker/src/handlers/create_partitions.rs`,
`crates/kaas-broker/src/topic_cr_writer.rs` (`expand_topic`).

**Verified by**: `scripts/kafka-topics.sh` (scenario 4, alter-and-describe);
writer unit tests in `crates/kaas-broker/src/topic_cr_writer.rs`.

## IncrementalAlterConfigs

Per-key topic config mutation ([KIP-339](../kip/kip-339.md)) —
`kafka-configs.sh --alter --add-config` / `--delete-config`.

**Versions**: v0–v1 (flexible from v1).

**Handling**: TOPIC resources only. The handler authorizes `AlterConfigs` on
the topic, translates the op list, and issues a single JSON-merge patch on
`KafkaTopic.spec.config`: `SET` writes the parsed value (integer keys become
JSON numbers), `DELETE` — and `SET` with a null value — write JSON null. The
patchable key set is the same six keys DescribeConfigs reports, accepted in
dotted or camelCase form. The operator materialises the change on reconcile
and the storage engine's cleaner picks it up. `validate_only` skips the patch.
`BROKER` and `BROKER_LOGGER` resource types answer a per-resource
`UNSUPPORTED_VERSION` (35) — there is no dynamic broker-config surface.

**Deviations from Apache 3.7**:

- **`APPEND` and `SUBTRACT` are unsupported** and answer
  `UNSUPPORTED_VERSION` (35): every kaas topic-config key is scalar, so the
  list-valued ops have nothing to apply to.
- Config keys outside the six-key allow-list answer `UNSUPPORTED_VERSION`
  (35), where Apache validates the key and returns `INVALID_CONFIG` for
  unknown names.
- `BROKER` / `BROKER_LOGGER` alteration is unsupported (Apache 3.7 supports
  dynamic broker configs, KIP-226).
- One bad op fails the whole resource — the ops for a resource are applied as
  a single all-or-nothing merge patch.
- The change is asynchronous, and — per the DescribeConfigs deviation above —
  a subsequent describe does not yet echo the override.

**Source**: `crates/kaas-broker/src/handlers/incremental_alter_configs.rs`,
`crates/kaas-broker/src/topic_cr_writer.rs` (`update_topic_config`,
`config_key_to_json_field`, `config_value_to_json`).

**Verified by**: `scripts/kafka-configs.sh` (scenario 3); key/value-mapping
unit tests in `crates/kaas-broker/src/topic_cr_writer.rs`.
