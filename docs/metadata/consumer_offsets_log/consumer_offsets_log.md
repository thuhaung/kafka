# Consumer Offsets Log Design

## Purpose

The consumer offsets log is the durable metadata log owned by partitions of the
internal `__consumer_offsets` topic.

It stores consumer-group coordination facts, not user topic records. Each
`__consumer_offsets` partition owns an independent segmented append-only log.
Consumer group state is recovered by replaying the partition that owns the
group.

The record types stored by this log are defined in [types.md](./types.md).

## Ownership Model

Every consumer group maps to exactly one partition of `__consumer_offsets`.
The group coordinator for that group is the broker that currently leads the
assigned partition.

The assignment rule is:

```text
hash(group.id) % number_of___consumer_offsets_partitions
```

The computed partition ID identifies the only `__consumer_offsets` partition
that may store records for the group. A broker must only append records for a
group when it is the leader of that assigned partition.

The hash algorithm must be deterministic across brokers and process restarts.
Until the exact algorithm is selected, implementations should inject or isolate
the hash function so tests can assert stable ownership behavior.

## Relationship to the Normal Log

The consumer offsets log reuses the normal segmented log design:

- Segment structure, active/inactive state, file naming, and rollover are
  defined in [segment.md](../../storage/log/segment.md)
- The `.log` file framing, append semantics, durability policy, and truncated
  tail recovery are defined in [log.md](../../storage/log/log.md)
- Offset lookup behavior is defined in [index.md](../../storage/log/index.md)
- Timestamp lookup behavior is defined in [timeindex.md](../../storage/log/timeindex.md)

Because this log is owned by `__consumer_offsets` partitions, files should live
under the corresponding internal topic partition directory. Example layout for
partition `0`:

- `__consumer_offsets-0/00000000000000000000.log`
- `__consumer_offsets-0/00000000000000000000.index`
- `__consumer_offsets-0/00000000000000000000.timeindex`

This document defines the log-level behavior. The consumer-offsets-specific
record body stored inside the normal `.log` record frame is defined in
[types.md](./types.md).

## Record Framing

Each consumer offsets log entry uses the same outer framing as a normal `.log`
record:

1. Offset: 8 bytes
2. Message length: 4 bytes
3. Serialized consumer offsets record body: `Message length` bytes

The serialized record body is:

1. Type: the consumer offsets record type
2. Data: the payload for that record type

Recommended body layout:

1. Type: 1 byte
2. Data length: 4 bytes
3. Data bytes: `Data length` bytes

The `Message length` field from the outer frame is the length of `Type`, `Data
length`, and `Data` together. Numeric fields should use the same byte order as
the normal log serializer; big-endian is the recommended default.

## Replay Model

On startup or leadership acquisition for a `__consumer_offsets` partition, the
broker rebuilds local coordinator state by replaying that partition's consumer
offsets log in offset order.

Replay rules:

1. Start with an empty coordinator state for the partition.
2. Apply `ConsumerGroupRegistrationRecord` to create or refresh group metadata.
3. Apply `ConsumerRegistrationRecord` to create or update member state within
   an existing group.
4. Apply `ConsumerOffsetCommitRecord` to update the committed offset for
   `(GroupID, Topic, PartitionID)`.
5. Stop and return an error if a record type is unknown or a record cannot be
   safely decoded.

If a member or offset commit record is encountered before its group
registration, replay should fail. The coordinator must append group
registration before member registration or offset commits.

## Compaction Keys

The consumer offsets log is append-only for writes, but it may be compacted by
record key once the log cleaner is implemented.

Recommended keys:

| Record | Compaction key |
| --- | --- |
| `ConsumerGroupRegistrationRecord` | `group:{GroupID}` |
| `ConsumerRegistrationRecord` | `member:{GroupID}:{MemberID}` |
| `ConsumerOffsetCommitRecord` | `offset:{GroupID}:{Topic}:{PartitionID}` |

Compaction must preserve the latest valid record for each key. Deletion
tombstones and group expiration are out of scope for this phase.

## Failure Behavior

The coordinator must explicitly handle:

- append failure while writing group registration records
- append failure while writing consumer registration records
- append failure while writing offset commit records
- leadership loss for the assigned `__consumer_offsets` partition
- replay failure caused by malformed or unknown records

A broker must not expose a new in-memory group, member, or committed offset as
accepted until the corresponding consumer offsets log record has been durably
accepted by the assigned partition log.
