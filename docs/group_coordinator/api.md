# Group Coordinator APIs

## Purpose

This document defines the consumer-facing APIs owned by the group coordinator.
The exact transport encoding is deferred to protocol design, but the logical
request and response shapes are authoritative here.

The consumer must treat the coordinator returned by `FindCoordinator` as the
target for `JoinGroup`, `Fetch`, and `OffsetCommit` in this phase.

Durable metadata records appended to `__consumer_offsets` are defined in
[consumer_offsets_log.md](../metadata/consumer_offsets_log/consumer_offsets_log.md).

## API Summary

| API | Called By | Sent To | Purpose |
| --- | --- | --- | --- |
| `FindCoordinator` | consumer startup | bootstrap broker | Locate the broker assigned as coordinator for `group.id`. |
| `JoinGroup` | consumer startup | group coordinator | Join the consumer group; the coordinator assigns partitions and returns member identity plus assignments. |
| `Fetch` | polling loop | group coordinator | Fetch new messages for the consumer's assigned partitions. |
| `OffsetCommit` | polling loop | group coordinator | Commit the highest fetched offsets for assigned partitions. |

## `FindCoordinator`

`FindCoordinator` locates the broker assigned as the group coordinator for a
consumer group.

Request:

| Field | Type | Required | Description |
| --- | --- | --- | --- |
| `GroupID` | string | Yes | Consumer group identifier. |

Response:

| Field | Type | Required | Description |
| --- | --- | --- | --- |
| `Coordinator.NodeID` | int32 | Yes | Broker node ID for the group coordinator. |
| `Coordinator.Host` | string | Yes | Client-facing host for the coordinator. |
| `Coordinator.Port` | int32 | Yes | Client-facing port for the coordinator. |

Processing:

1. Validate that `GroupID` is non-empty.
2. Read `__consumer_offsets` topic metadata from the current metadata image.
3. If `__consumer_offsets` is missing, send an internal create-topic request to
   the leader controller using the default config: `5` partitions, replication
   factor `1`, and min ISR `1`.
4. Store the returned topic and partition metadata in the broker/coordinator's
   current metadata image or wait until the refreshed image contains it.
5. Compute the assigned `__consumer_offsets` partition ID as
   `hash(GroupID) % PartitionCount`.
6. Look up the leader broker ID for that partition.
7. Resolve the leader broker ID to broker endpoint metadata.
8. If the local broker is not the leader, return the resolved coordinator
   broker information without mutating local group state.
9. If the local broker is the leader and the group is unknown locally, append
   `ConsumerGroupRegistrationRecord` to the local consumer offsets log
   partition.
10. Add the group to in-memory coordinator state.
11. Return the local broker as coordinator.

The assigned `__consumer_offsets` partition ID is an internal coordinator
lookup detail. It must not be included in the consumer-facing response because
the consumer only needs the coordinator endpoint.

Failure behavior:

- Return an error when `__consumer_offsets` is missing from metadata.
- Return an error when the internal topic has zero partitions.
- Return an error when the assigned partition has no known leader.
- Return an error when leader broker endpoint metadata is unavailable.
- Return an error when the local coordinator cannot append
  `ConsumerGroupRegistrationRecord`.

Retry behavior:

- `FindCoordinator` is idempotent for the same `group.id`.
- A consumer may retry the same `FindCoordinator` after connection close,
  write timeout, or response read timeout, subject to the bounded retry policy
  in [protocol.md](../protocol/protocol.md).
- Retried requests must keep the same `GroupID`.
- If the request triggers lazy creation of `__consumer_offsets`, the internal
  create-topic request must use the idempotent internal create semantics
  defined in [topic types](../storage/topic/types.md).

The observable result of `FindCoordinator` is coordinator endpoint discovery.
If the coordinator already materialized the group but the response was lost, a
retry should find the same group and return the resolved coordinator endpoint
without appending duplicate group metadata.

## `JoinGroup`

`JoinGroup` registers a consumer as a member of a consumer group and returns its
member identity and assigned partitions.

Request:

| Field | Type | Required | Description |
| --- | --- | --- | --- |
| `GroupID` | string | Yes | Consumer group identifier. |
| `Topic` | string | Yes | Topic the consumer wants to consume. |

Response:

| Field | Type | Required | Description |
| --- | --- | --- | --- |
| `MemberID` | string | Yes | Coordinator-assigned member identifier. |
| `AssignedPartitionIDs` | list of int32 | Yes | Partition IDs assigned to the consumer for the requested topic. |

Processing:

1. Validate `GroupID` and `Topic`.
2. Verify that the local broker is coordinator for `GroupID`.
3. If the group is not present in local in-memory coordinator state, append
   `ConsumerGroupRegistrationRecord` and materialize the group locally.
4. Verify that the requested topic exists in the metadata image.
5. Assign topic partitions to the joining member.
6. Generate a coordinator-assigned member ID for the join response.
7. Append `ConsumerRegistrationRecord` to the consumer offsets log.
8. Update in-memory group member state.
9. Return the member ID and assigned partition IDs.

Partition assignment is coordinator-owned in this phase. Because rebalancing and
membership changes are not yet defined, a simple deterministic assignment is
acceptable, such as assigning all partitions of the requested topic to the first
member of the group and returning an empty assignment to later members. A later
rebalance design should replace this with a group-wide assignment protocol.

Failure behavior:

- Return a coordinator error when the local broker is not coordinator for the
  group.
- Return an error when the group cannot be materialized in the assigned
  consumer offsets log.
- Return an error when the requested topic does not exist.
- Return an error when partition assignment produces partition IDs outside the
  requested topic's metadata.
- Return an error when the member record cannot be appended.

Retry behavior:

- `JoinGroup` is not retryable after an unknown outcome in this phase.
- A consumer may retry `JoinGroup` only when connection setup fails before any
  request bytes are written.
- If the connection closes, the write deadline expires after bytes may have
  been written, or the response read timeout expires before a response, the
  consumer must fail startup with a clear unknown-outcome error instead of
  automatically sending another `JoinGroup`.

This restriction exists because stable member IDs, client-supplied member
identity, session timeouts, and duplicate join detection are not yet part of the
coordinator contract. Retrying after the coordinator appended
`ConsumerRegistrationRecord` but crashed before responding could register a
second logical member for the same consumer process.

## Consumer `Fetch`

Consumer `Fetch` returns records for the consumer's assigned partitions.

Request:

| Field | Type | Required | Description |
| --- | --- | --- | --- |
| `GroupID` | string | Yes | Consumer group identifier. |
| `MemberID` | string | Yes | Coordinator-assigned member identifier. |
| `Topic` | string | Yes | Topic name. |
| `PartitionIDs` | list of int32 | Yes | Assigned partitions to fetch from. |

Response:

| Field | Type | Required | Description |
| --- | --- | --- | --- |
| `RecordsByPartition` | map int32 to records | Yes, may be empty | Records returned for each requested partition. |

Processing:

1. Validate `GroupID`, `MemberID`, and `Topic`.
2. Verify that the local broker is coordinator for `GroupID`.
3. Verify that `MemberID` belongs to the group.
4. Verify that every requested partition is assigned to that member.
5. For each requested partition, read records from the appropriate partition
   log or route internally to the partition leader when that behavior is added.
6. Return records for each requested partition.

The response does not need a `HighestFetchedOffsets` field. Records already
carry offsets, so the consumer can compute the highest fetched offset per
partition from `RecordsByPartition` after each successful fetch. Empty
partition responses do not advance the consumer's tracked offset.

Failure behavior:

- Return a coordinator error when the local broker is not coordinator for the
  group.
- Return an error when the member is unknown.
- Return an error when the request includes a partition not assigned to the
  member.
- Return an error when a requested topic partition does not exist.
- Return an error when records cannot be fetched from the partition log.

Retry behavior:

- Consumer `Fetch` is idempotent for the same
  `group.id + member.id + topic + partition IDs` request.
- A consumer may retry the same `Fetch` after connection close, write timeout,
  or response read timeout, subject to the bounded retry policy in
  [protocol.md](../protocol/protocol.md).
- Retried requests must preserve the same member identity and assigned
  partition set. The consumer must not advance `HighestFetchedOffset` until a
  valid response is decoded.

`Fetch` does not mutate coordinator state in this phase. If a response is lost,
retrying may return the same records again; consumers must use record offsets
to update local position only after successful decode.

## `OffsetCommit`

`OffsetCommit` stores the consumed offset for a consumer group member and topic
partition.

Request:

| Field | Type | Required | Description |
| --- | --- | --- | --- |
| `GroupID` | string | Yes | Consumer group identifier. |
| `MemberID` | string | Yes | Coordinator-assigned member identifier. |
| `Topic` | string | Yes | Topic name. |
| `PartitionID` | int32 | Yes | Partition being committed. |
| `Offset` | int64 | Yes | Highest fetched offset being committed. |

Response:

| Field | Type | Required | Description |
| --- | --- | --- | --- |
| `Committed` | bool | Yes | Whether the offset commit was accepted. |

The implementation may batch multiple partition commits into one request if the
wire protocol supports it. The logical commit unit remains
`group.id + member.id + topic + partition + offset`.

Processing:

1. Validate `GroupID`, `MemberID`, `Topic`, `PartitionID`, and `Offset`.
2. Verify that the local broker is coordinator for `GroupID`.
3. Verify that `MemberID` belongs to the group.
4. Verify that the partition is assigned to the member.
5. Append `ConsumerOffsetCommitRecord` to the consumer offsets log.
6. Update in-memory committed offset state.
7. Return success.

Failure behavior:

- Return a coordinator error when the local broker is not coordinator for the
  group.
- Return an error when the member is unknown.
- Return an error when the partition is not assigned to the member.
- Return an error when the offset is negative.
- Return an error when the commit record cannot be appended.

Retry behavior:

- `OffsetCommit` is idempotent for an identical
  `group.id + member.id + topic + partition + offset` request.
- A requester may retry the same `OffsetCommit` after a connection close, write
  timeout, or response read timeout, subject to the bounded retry policy in
  [protocol.md](../protocol/protocol.md).
- Retried `OffsetCommit` requests must not change the offset or any identity
  field from the original request.

The committed offset is an absolute position, not a delta. If the first attempt
append succeeds but the response is lost, a retry that appends the same
`ConsumerOffsetCommitRecord` leaves the materialized committed offset at the
same value after replay. Duplicate identical commit records are therefore
acceptable in this phase.
