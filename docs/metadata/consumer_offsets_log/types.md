# Consumer Offsets Log Types

## Purpose

This document defines the durable record types stored in the
`__consumer_offsets` log. The log-level behavior, ownership, framing, replay,
and failure rules are defined in
[consumer_offsets_log.md](./consumer_offsets_log.md).

## Record Envelope

Each consumer offsets record body is wrapped in a small envelope:

| Field | Type | Required | Description |
| --- | --- | --- | --- |
| `Type` | uint8 | Yes | Consumer offsets record type ID |
| `DataLength` | int32 | Yes | Length of the serialized type-specific payload |
| `Data` | bytes | Yes | Type-specific payload |

Unknown record types must be rejected during append. During recovery, an
unknown type should stop replay and return an error because the broker cannot
safely interpret the coordinator state that follows.

## Record Types

The first version supports these concrete record types:

| Type ID | Type | Meaning |
| --- | --- | --- |
| `1` | `ConsumerGroupRegistrationRecord` | Registers a consumer group on its assigned `__consumer_offsets` partition |
| `2` | `ConsumerRegistrationRecord` | Registers a consumer group member and its topic partition assignment |
| `3` | `ConsumerOffsetCommitRecord` | Stores a committed offset for a group member and topic partition |

## ConsumerGroupRegistrationRecord

`ConsumerGroupRegistrationRecord` records that a consumer group has been
materialized by the coordinator that leads the assigned `__consumer_offsets`
partition.

Payload fields:

| Field | Type | Required | Description |
| --- | --- | --- | --- |
| `GroupID` | string | Yes | Consumer group identifier |
| `ConsumerOffsetsPartitionID` | int32 | Yes | Partition ID in `__consumer_offsets` assigned to the group |
| `CreatedAt` | timestamp | Yes | Time the coordinator first materialized the group |
| `Coordinator.NodeID` | int32 | Yes | Broker node ID for the coordinator |
| `Coordinator.Host` | string | Yes | Client-facing host for the coordinator |
| `Coordinator.Port` | int32 | Yes | Client-facing port for the coordinator |

Validation rules:

- `GroupID` must be non-empty
- `ConsumerOffsetsPartitionID` must match the partition that owns the physical
  log receiving the record
- `CreatedAt` must be set
- `Coordinator.NodeID` must identify the broker that leads the assigned
  `__consumer_offsets` partition at append time
- `Coordinator.Host` must be non-empty
- `Coordinator.Port` must be greater than zero

Recommended `Data` layout:

1. GroupID length: 4 bytes
2. GroupID bytes: variable length
3. ConsumerOffsetsPartitionID: 4 bytes
4. CreatedAt: 8 bytes
5. Coordinator.NodeID: 4 bytes
6. Coordinator.Host length: 4 bytes
7. Coordinator.Host bytes: variable length
8. Coordinator.Port: 4 bytes

`CreatedAt` should be encoded as Unix time in milliseconds since epoch.

## ConsumerRegistrationRecord

`ConsumerRegistrationRecord` records a consumer member joining a group and the
topic partitions assigned to that member.

Payload fields:

| Field | Type | Required | Description |
| --- | --- | --- | --- |
| `GroupID` | string | Yes | Consumer group identifier |
| `MemberID` | string | Yes | Coordinator-assigned member identifier |
| `Topic` | string | Yes | Topic requested by the consumer |
| `AssignedPartitionIDs` | list of int32 | Yes, may be empty | Topic partitions assigned to this member |
| `JoinedAt` | timestamp | Yes | Time the member joined |

Validation rules:

- `GroupID` must be non-empty
- `GroupID` must map to the `__consumer_offsets` partition receiving the record
- `MemberID` must be non-empty
- `Topic` must be non-empty
- `AssignedPartitionIDs` must not contain duplicates
- Every assigned partition ID must be greater than or equal to zero
- `JoinedAt` must be set

Recommended `Data` layout:

1. GroupID length: 4 bytes
2. GroupID bytes: variable length
3. MemberID length: 4 bytes
4. MemberID bytes: variable length
5. Topic length: 4 bytes
6. Topic bytes: variable length
7. AssignedPartitionIDs count: 4 bytes
8. Repeated AssignedPartitionIDs entries:
   - PartitionID: 4 bytes
9. JoinedAt: 8 bytes

`JoinedAt` should be encoded as Unix time in milliseconds since epoch.

## ConsumerOffsetCommitRecord

`ConsumerOffsetCommitRecord` records a committed offset for one logical
`group.id + member.id + topic + partition` commit.

Payload fields:

| Field | Type | Required | Description |
| --- | --- | --- | --- |
| `GroupID` | string | Yes | Consumer group identifier |
| `MemberID` | string | Yes | Coordinator-assigned member identifier |
| `Topic` | string | Yes | Topic name |
| `PartitionID` | int32 | Yes | Topic partition being committed |
| `Offset` | int64 | Yes | Highest fetched offset being committed |
| `CommittedAt` | timestamp | Yes | Time the offset was committed |

Validation rules:

- `GroupID` must be non-empty
- `GroupID` must map to the `__consumer_offsets` partition receiving the record
- `MemberID` must be non-empty
- `Topic` must be non-empty
- `PartitionID` must be greater than or equal to zero
- `Offset` must be greater than or equal to zero
- `CommittedAt` must be set

Recommended `Data` layout:

1. GroupID length: 4 bytes
2. GroupID bytes: variable length
3. MemberID length: 4 bytes
4. MemberID bytes: variable length
5. Topic length: 4 bytes
6. Topic bytes: variable length
7. PartitionID: 4 bytes
8. Offset: 8 bytes
9. CommittedAt: 8 bytes

`CommittedAt` should be encoded as Unix time in milliseconds since epoch.

The latest valid commit record for a `(GroupID, Topic, PartitionID)` key is the
current committed offset for that group and topic partition.
