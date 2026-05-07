# Cluster Metadata Design

## Purpose

Cluster metadata is the in-memory view that every broker builds from the
cluster metadata log.

The cluster metadata log, defined in
[cluster_metadata_log.md](../cluster_metadata_log/cluster_metadata_log.md),
stores ordered metadata records. This document defines the materialized state
produced by replaying those records and the read APIs exposed to the rest of
the broker process.

Recommended Go types for this image are defined in [types.md](./types.md).

Every broker builds this metadata image, whether it is currently acting as the
controller or as a non-controller broker. The controller uses the image to
validate and apply new control-plane changes. Non-controller brokers use the
same image to route topic, partition, produce, fetch, and replication work to
the correct broker.

## Goals

The metadata image must provide:

- Topic metadata keyed by topic name
- Partition metadata keyed by topic name and partition ID
- Broker metadata keyed by broker ID
- Deterministic reconstruction by replaying the cluster metadata log in offset
  order
- Lookup APIs for fetching partitions of a topic
- Lookup APIs for finding the leader broker ID of a partition
- Lookup APIs for fetching broker information

## Non-Goals

The initial metadata image does not attempt to define:

- The segmented-log mechanics used to store metadata records
- Metadata snapshot files
- Metadata compaction
- Consumer group metadata or committed offsets; those are defined in
  [consumer_offsets_log.md](../consumer_offsets_log/consumer_offsets_log.md)
- ACLs or authorization metadata
- Transaction metadata
- Broker heartbeat, shutdown, or fencing metadata beyond startup registration
- Raft replication mechanics for the metadata log

Those concerns can be layered on without changing the core rule that brokers
construct a read-only metadata image from committed metadata-log records.

## Relationship to the Cluster Metadata Log

The metadata image is derived from records stored in the internal
`__cluster_metadata` topic.

The log's record types are defined in
[types.md](../cluster_metadata_log/types.md):

- `TopicCreationRecord`
- `PartitionCreationRecord`
- `PartitionUpdateRecord`
- `BrokerRegistrationRecord`

Broker metadata is reconstructed from `BrokerRegistrationRecord` entries in the
same way that topic and partition metadata are reconstructed from their record
types. Bootstrap configuration may still be used to contact the controller
quorum before the image is fetched, but broker routing metadata should come from
the image once registration records have been replayed.

Replay rules:

1. Start from an empty metadata image.
2. Read committed metadata-log records in increasing offset order.
3. Validate each record against the current image.
4. Apply the record to produce the next image state.
5. Reject or stop replay on records that cannot be safely interpreted.

The image must expose only state that has been applied from committed records or
trusted bootstrap configuration. Uncommitted controller-local decisions must not
be visible through this API.

## Metadata Image

The metadata image is the complete read model held by a broker process at a
specific metadata-log offset.

Required top-level fields:

| Field | Type | Required | Description |
| --- | --- | --- | --- |
| `ClusterID` | string | Yes | Cluster identifier shared by all nodes in the same cluster |
| `AppliedOffset` | int64 | Yes | Highest cluster metadata log offset included in this image. `-1` means no records have been applied |
| `Topics` | map string to `TopicMetadata` | Yes | Topic metadata keyed by topic name |
| `Partitions` | map `PartitionKey` to `PartitionMetadata` | Yes | Partition metadata keyed by topic name and partition ID |
| `Brokers` | map int32 to `BrokerMetadata` | Yes | Broker metadata keyed by broker ID |

The metadata image should be treated as immutable by callers. Updates should be
applied through a metadata store or builder that validates metadata records and
then swaps in the resulting state.

## Topic Metadata

Topic metadata describes a topic as a cluster-level object.

The topic name is the unique topic key. A separate topic ID is not required in
this phase because topic names are already validated as unique.

Fields:

| Field | Type | Required | Description |
| --- | --- | --- | --- |
| `Name` | string | Yes | Unique topic name |
| `Config` | `metadata.TopicConfig` | Yes | Minimal stored topic-level configuration used until the topic package owns a concrete config type |
| `CreatedAt` | timestamp | Yes | Time the topic metadata entry was created |

The first implementation should define a minimal `metadata.TopicConfig` with
the topic fields needed by metadata replay and startup reads. When the topic
component is implemented, the metadata image should either reuse that component's
config type directly or keep `metadata.TopicConfig` aligned with it. The topic
partition count is read from `Config.Partitions`.

## Partition Metadata

Partition metadata describes the placement and leadership of one topic
partition.

The partition key is:

| Field | Type | Required | Description |
| --- | --- | --- | --- |
| `TopicName` | string | Yes | Topic that owns the partition |
| `PartitionID` | int32 | Yes | Partition number within the topic |

Fields:

| Field | Type | Required | Description |
| --- | --- | --- | --- |
| `TopicName` | string | Yes | Topic that owns the partition |
| `PartitionID` | int32 | Yes | Partition number within the topic |
| `LeaderBrokerID` | int32 | Yes | Broker currently responsible for leader duties |
| `ReplicaBrokerIDs` | array of int32 | Yes | Brokers assigned as replicas for the partition |
| `ISRBrokerIDs` | array of int32 | Yes, may be empty during creation | Brokers currently considered in-sync replicas |
| `CreatedAt` | timestamp | Yes for creation | Time the partition metadata entry was created |
| `UpdatedAt` | timestamp | Yes for updates | Time the partition metadata entry was last updated |

## Broker Metadata

Broker metadata describes how to identify and contact a broker in the cluster.

Broker ID is the unique broker key. It must match the node ID loaded from the
broker's properties file supplied by `config.path`.

Fields:

| Field | Type | Required | Description |
| --- | --- | --- | --- |
| `BrokerID` | int32 | Yes | Unique broker node ID |
| `Host` | string | Yes | Advertised broker host for client and broker traffic |
| `Port` | int32 | Yes | Advertised broker port for client and broker traffic |
| `ControllerHost` | string | No | Host for broker-controller traffic when different from `Host` |
| `ControllerPort` | int32 | No | Port for broker-controller traffic when different from `Port` |
| `LogDir` | string | No | Broker-local log directory from node configuration |
| `RegisteredAt` | timestamp | No | Time the broker metadata entry was created |
| `UpdatedAt` | timestamp | No | Time the broker metadata entry was last updated |

Application rules:

- `BrokerRegistrationRecord` creates a new `BrokerMetadata` entry when the
  broker ID is not present
- Replaying any `BrokerRegistrationRecord` for an existing broker ID is invalid
  in this phase. Broker restart idempotency is handled by the leader controller
  before append: if a startup registration request matches the existing broker
  metadata, the controller returns the current image without appending another
  record.
- Later, heartbeat, shutdown, fencing, or explicit broker update records can
  extend broker metadata through new cluster metadata log record types
- Partition metadata should reference broker IDs, not host and port pairs
- Routing code should resolve broker IDs to network endpoints by using this
  metadata image

## Read API

The metadata package should expose a read-only API close to:

```go
type Reader interface {
	PartitionsForTopic(topicName string) ([]PartitionMetadata, error)
	LeaderBrokerID(topicName string, partitionID int32) (int32, error)
	Broker(brokerID int32) (BrokerMetadata, error)
}
```

`PartitionsForTopic(topicName)` returns all known partitions for a topic,
sorted by `PartitionID` in ascending order. It should return `ErrTopicNotFound`
when the topic does not exist and return defensive copies of partition
metadata.

`LeaderBrokerID(topicName, partitionID)` returns the current leader broker ID
for one partition. It should return `ErrTopicNotFound` when the topic does not
exist and `ErrPartitionNotFound` when the partition key does not exist.

`Broker(brokerID)` returns contact and status metadata for one broker. It
should return `ErrBrokerNotFound` when the broker ID does not exist and return a
defensive copy of `BrokerMetadata`.

## Error Model

The metadata package should expose stable errors so callers can distinguish
missing metadata from internal corruption.

Recommended errors:

```go
var (
	ErrTopicNotFound     = errors.New("topic not found")
	ErrPartitionNotFound = errors.New("partition not found")
	ErrBrokerNotFound    = errors.New("broker not found")
	ErrInvalidMetadata   = errors.New("invalid metadata")
)
```

Validation and replay failures should wrap `ErrInvalidMetadata` with contextual
information such as topic name, partition ID, broker ID, record type, and
metadata-log offset.

## Indexes

The image should maintain indexes that make the required read APIs efficient.

Required indexes:

| Index | Key | Value | Used By |
| --- | --- | --- | --- |
| `TopicsByName` | topic name | `TopicMetadata` | Topic lookup and validation |
| `PartitionsByKey` | `(topic name, partition ID)` | `PartitionMetadata` | Leader and partition lookup |
| `PartitionsByTopic` | topic name | sorted partition IDs or partition metadata | Fetching partitions for a topic |
| `BrokersByID` | broker ID | `BrokerMetadata` | Broker endpoint lookup |

`PartitionsByTopic` can be derived from `PartitionsByKey`, but maintaining it
as an index is useful because partition discovery is a common operation.

## Concurrency

Metadata reads will be frequent and should not block on long replay work.

Recommended approach:

- Keep the current `Image` immutable after publication
- Build updates in a separate builder or copy-on-write structure
- Publish the new image atomically after records are applied
- Let readers hold a stable image pointer for the duration of a request
- Deep-copy images, maps, slices, and nested metadata before returning them to
  broker or controller startup callers

This keeps produce, fetch, replication, and metadata-read paths from observing
partially applied metadata.

## Startup and Recovery

Broker startup should construct metadata in this order:

1. Load local node metadata from the properties file supplied by `config.path`.
2. Use bootstrap controller quorum information from configuration to contact a
   controller.
3. Register with the leader controller and fetch the current metadata image.
4. Replay any local committed metadata-log records that are newer than the
   fetched image when such local catch-up is supported.
5. Publish the resulting metadata image.
6. Use the image for topic, partition, leader, and broker lookups.

If replay fails, the broker must not serve requests using a partial image unless
the failure mode is explicitly handled by a later recovery design.

## Testing Strategy

Unit tests should cover:

- Building an empty metadata image
- Applying all record types defined in
  [types.md](../cluster_metadata_log/types.md)
- Rejecting duplicate topics
- Rejecting duplicate partitions
- Rejecting partitions for unknown topics
- Rejecting partition leaders outside the replica set
- Creating broker metadata from broker registration
- Rejecting broker registration for a mismatched cluster ID
- Rejecting duplicate broker registration records for an existing broker ID
- Returning partitions for a topic sorted by partition ID
- Returning leader broker IDs for existing partitions
- Returning `ErrTopicNotFound`, `ErrPartitionNotFound`, and
  `ErrBrokerNotFound`
- Returning defensive copies from read APIs
- Preserving a previous image when applying an invalid update fails
