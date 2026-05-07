# Topic Types

## Purpose

This document defines the topic metadata model, internal topic lifecycle rules,
and recommended Go types for `internal/topic`.

## Topic Metadata Model

Each topic metadata record should include:

| Field | Type | Required | Description |
| --- | --- | --- | --- |
| Name | string | Yes | Unique topic name |
| Partitions | integer | Yes | Number of partitions for the topic. Defaults to `1` if not provided by the caller |
| ReplicationFactor | integer | Yes | Number of brokers that should host each partition replica |
| RetentionMs | integer | No | Maximum age of retained log data in milliseconds |
| RetentionBytes | integer | No | Maximum retained log size in bytes |
| MinInSyncReplicas | integer | Yes | Minimum number of in-sync replicas required for successful produce acknowledgement |

The user's stated required metadata is the topic `Name` plus the configs above.
This document treats `retention.ms` and `retention.bytes` as the initial
retention parameters because they are the most direct and useful retention
controls for a Kafka-like system.

## Cluster-Created Topics

Kafka-compatible clusters use internal topics for control-plane and coordination
state. These topics are reserved by the system and are not ordinary user-created
topics.

The initial cluster-created topics are:

| Topic | Responsibility | Used By |
| --- | --- | --- |
| `__consumer_offsets` | Stores consumer group, consumer, and committed offset metadata defined in [consumer_offsets_log.md](../../metadata/consumer_offsets_log/consumer_offsets_log.md) | The consumer group coordinator uses this topic to update consumed offsets per partition, assign partitions to consumers, and track consumer group state |
| `__cluster_metadata` | Stores metadata about topics, brokers, and controllers | The quorum uses this topic to create topics and update partition leadership |

The topic component should recognize these names as reserved internal topics
during validation. System callers may create or materialize them through
bootstrap or controller-owned internal flows, but normal topic-create requests
from the CLI or public API must not be allowed to create, alter, delete, or
overwrite them.

## `__cluster_metadata` Lifecycle

`__cluster_metadata` must always exist for a cluster. A controller must be able
to open the cluster metadata log before it can replay topic metadata, accept
broker registrations, or process topic creation requests. Because of that, this
topic is not created by appending an ordinary `TopicCreationRecord` to itself.

The initial implementation should treat `__cluster_metadata` as a bootstrap
descriptor persisted with controller-local configuration:

- The descriptor is loaded from the controller's configured
  `metadata.log.dir`, as defined in [types.md](../../quorum/types.md).
- If the descriptor is missing for a new cluster, the controller initializes it
  using the built-in defaults below and persists it before opening the metadata
  log.
- If the descriptor exists, every controller validates that it matches the
  built-in invariants before serving metadata traffic.
- If the descriptor conflicts with the controller's `ClusterID` or fixed
  partitioning rules, startup fails rather than silently creating a second
  metadata history.

Recommended first-phase descriptor:

| Field | Value |
| --- | --- |
| Name | `__cluster_metadata` |
| Partitions | `1` |
| Partition IDs | `0` only |
| Replication model | Controller quorum replication, not normal broker topic replication |
| Retention | Unlimited until metadata snapshots or compaction are designed |
| Compaction | Disabled in the first phase |
| Placement | Controller `metadata.log.dir` |

Partitioning is intentionally fixed to one partition in this phase. Cluster
metadata changes must be totally ordered, and a single `__cluster_metadata-0`
log gives the controller quorum one authoritative sequence of topic, partition,
and broker records. Later Raft work may replace the static leader model, but it
should preserve the single logical metadata log unless a separate design
introduces metadata sharding.

The in-memory metadata image may expose `__cluster_metadata` as an internal
topic so broker and diagnostic reads can see that it exists. That exposure
should be synthesized from the bootstrap descriptor, not accepted from a public
topic-create request.

## `__consumer_offsets` Lifecycle

`__consumer_offsets` does not need to exist when a brand-new cluster starts. It
is needed only when consumer group coordination begins.

The first implementation should create it lazily during `FindCoordinator`:

1. A consumer contacts any broker with `FindCoordinator(group.id)`.
2. The contacted broker reads the current metadata image.
3. If `__consumer_offsets` exists, the broker uses its partition count to map
   the group to a coordinator partition.
4. If `__consumer_offsets` is missing, the broker sends an internal create-topic
   request to the leader controller.
5. The leader controller validates that the request is from a system caller,
   creates the topic metadata, creates its partition metadata, commits those
   records to `__cluster_metadata`, and publishes the resulting metadata image.
6. The contacted broker refreshes or waits for a metadata image that contains
   `__consumer_offsets`.
7. The broker computes
   `hash(group.id) % __consumer_offsets.Partitions`, resolves the leader for
   that partition, and returns the coordinator endpoint.

Recommended first-phase defaults:

| Field | Value |
| --- | --- |
| Name | `__consumer_offsets` |
| Partitions | `5` |
| ReplicationFactor | `1` in the first phase |
| MinInSyncReplicas | `1` in the first phase |
| Retention | Unlimited until group expiration is designed |
| Compaction | Enabled once the log cleaner supports compaction keys |

The `__consumer_offsets` partition count becomes part of the group ownership
contract because every broker computes coordinator ownership with the same
`hash(group.id) % partitionCount` rule. Increasing this partition count later is
a rebalance and migration problem, so the initial implementation should treat
the value as immutable after creation.

The internal create request should be idempotent. If two consumers trigger
creation concurrently, exactly one controller operation should create the topic,
and the other request should observe the already-created internal topic and
continue coordinator lookup.

## Proposed Go Types

The exact package path may evolve, but `internal/topic` should expose types
close to:

```go
package topic

type Config struct {
	Partitions        int32
	ReplicationFactor int16
	RetentionMs       *int64
	RetentionBytes    *int64
	MinInSyncReplicas int16
}

type Metadata struct {
	Name   string
	Config Config
}

type CreateRequest struct {
	Name   string
	Config Config
}
```

Implementation notes:

- `Partitions` defaults to `1` when omitted by the caller.
- Pointer retention fields distinguish unset from explicitly configured values.
- `Metadata` contains only the fields required by current scope.
