# Cluster Metadata Log Design

## Purpose

The cluster metadata log records changes to cluster metadata in an append-only
log.

It extends the normal segmented log design documented under
[`docs/storage/log`](../../storage/log) instead of redefining segment mechanics here. The
metadata log uses the same segment lifecycle, rollover rules, index
relationship, record framing, and recovery behavior as the normal log. The
difference is the meaning of the serialized payload: cluster metadata log
records describe topic, partition, and broker metadata changes instead of user
messages.

The record types stored by this log are defined in [types.md](./types.md).

Metadata logs are owned by partitions of the internal `__cluster_metadata`
topic. Each `__cluster_metadata` partition has its own segmented metadata log,
following the same partition ownership model as normal topic partitions.

The cluster metadata log does not store consumer or consumer group metadata.
Consumer group state, including committed offsets, is broker-managed through
the internal `__consumer_offsets` topic, whose log is defined in
[consumer_offsets_log.md](../consumer_offsets_log/consumer_offsets_log.md).

## Goals

The metadata log design must provide:

- Append-only storage for metadata changes
- Offset-based ordering of metadata updates
- Ownership by partitions of the internal `__cluster_metadata` topic
- A small metadata record envelope with `Type` and `Data`
- Topic metadata keyed by topic name
- Partition metadata keyed by topic name and partition ID
- Broker metadata keyed by broker ID
- Replay from log records into an in-memory metadata image

## Non-Goals

The initial metadata log design does not attempt to define:

- User data storage
- Consumer metadata or consumer group metadata
- ACLs or authorization metadata
- Transaction metadata
- Snapshot files
- Metadata compaction
- Raft replication details

Those can be added later without changing the core idea that metadata changes
are persisted as ordered records in a segmented log.

## Relationship to the Normal Log

The metadata log should reuse the normal log designs by reference:

- Segment structure, active/inactive state, file naming, and rollover are
  defined in [segment.md](../../storage/log/segment.md)
- The `.log` file framing, append semantics, durability policy, and truncated
  tail recovery are defined in [log.md](../../storage/log/log.md)
- Offset lookup behavior is defined in [index.md](../../storage/log/index.md)
- Timestamp lookup behavior is defined in [timeindex.md](../../storage/log/timeindex.md)

The metadata log uses the same segment file trio:

- `.log`
- `.index`
- `.timeindex`

Because the metadata log is owned by `__cluster_metadata` partitions, its files
should live under the corresponding internal topic partition directory. Example
layout for partition `0`:

- `__cluster_metadata-0/00000000000000000000.log`
- `__cluster_metadata-0/00000000000000000000.index`
- `__cluster_metadata-0/00000000000000000000.timeindex`

This document only defines the log-level behavior. The metadata-specific record
body stored inside the normal `.log` record frame is defined in
[types.md](./types.md).

## Record Framing

Each metadata log entry uses the same outer framing as a normal `.log` record:

1. Offset: 8 bytes
2. Message length: 4 bytes
3. Serialized metadata record body: `Message length` bytes

The serialized metadata record body is:

1. Type: the type of metadata update
2. Data: the payload for that record type

Recommended body layout:

1. Type: 1 byte
2. Data length: 4 bytes
3. Data bytes: `Data length` bytes

The `Message length` field from the outer frame is the length of `Type`, `Data
length`, and `Data` together. Numeric fields should use the same byte order as
the normal log serializer; big-endian is the recommended default.

## Append Semantics

Metadata records are appended only to the active metadata segment.

Append behavior follows the normal log append semantics, with metadata-specific
validation before persistence:

- The metadata coordinator assigns the next metadata offset
- The record type is validated
- The type-specific `Data` payload is validated
- The serialized record is appended to the active segment's `.log` file
- Companion `.index` and `.timeindex` entries are written for the appended
  record

The metadata log must preserve the exact order of accepted metadata changes.
Replay depends on this ordering to reconstruct the latest topic, partition, and
broker state.

## Replay Model

On startup, the broker reconstructs metadata state by replaying the metadata log
from the lowest segment base offset to the highest segment base offset.

Replay applies records in offset order:

1. Read the next metadata log entry using the normal `.log` framing.
2. Decode the metadata record envelope.
3. Decode the type-specific `Data` payload.
4. Validate the decoded record against the metadata image built so far.
5. Apply the record to the in-memory metadata image.

Replay behavior by type:

- `TopicCreationRecord` creates the topic entry keyed by `Name`
- `PartitionCreationRecord` creates the partition entry keyed by
  `(TopicName, PartitionId)`
- `PartitionUpdateRecord` replaces the current partition state for
  `(TopicName, PartitionId)`
- `BrokerRegistrationRecord` creates the broker entry keyed by `BrokerID`

If replay sees a partition record for an unknown `TopicName`, replay should
fail. This prevents the broker from accepting partition metadata without a
corresponding topic creation record.

If replay sees a broker registration for a different `ClusterID`, replay should
fail. This prevents a broker from constructing an image from metadata belonging
to another cluster.

## Current State Derivation

The metadata log stores changes, not a precomputed view.

The current metadata image is derived from replay:

- The set of topics is built from all valid `TopicCreationRecord` records
- The set of partitions is built from all valid `PartitionCreationRecord`
  records
- The latest `PartitionUpdateRecord` for each `(TopicName, PartitionId)` defines
  that partition's current leader, replica assignment, and ISR
- Each valid `BrokerRegistrationRecord` creates one broker entry. A second
  registration record for an existing `BrokerID` is invalid in this phase. A
  broker restart with identical metadata is handled by returning the current
  image without appending a second registration record.

Because there is no compaction or snapshotting yet, startup must replay all
metadata log segments.

## Recovery Behavior

The metadata log follows the recovery behavior defined by the normal `.log`
design.

A metadata record is complete only when both the normal outer frame and the
metadata body are complete:

- 8-byte offset
- 4-byte message length
- `Message length` bytes of metadata body
- Inside the metadata body, the complete `Type`, `Data length`, and `Data`

After truncated tail repair, the broker replays the surviving metadata records
to rebuild the in-memory metadata image.

## Testing Strategy

Tests for the metadata log should cover:

- Appending and reading all record types defined in [types.md](./types.md)
- Rejecting duplicate topic names
- Rejecting partition records for unknown topics
- Rejecting unknown record types
- Rejecting invalid topic creation payloads
- Rejecting invalid base partition payloads
- Rejecting partition creation for an existing partition key
- Rejecting partition updates for an unknown partition key
- Rejecting invalid ISR updates
- Rejecting broker registration for a mismatched cluster ID
- Rejecting broker registration with invalid endpoint metadata
- Rejecting duplicate broker registration records for an existing broker ID
- Replay from multiple segments
- Latest partition update winning during replay
- Truncated tail recovery through the normal log recovery path
