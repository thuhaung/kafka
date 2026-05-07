# Group Coordinator Testing

## Purpose

This document defines validation rules, failure modes, test expectations, and
known unresolved design issues for the group coordinator.

Durable `__consumer_offsets` metadata record schemas and replay rules are
defined in [types.md](../metadata/consumer_offsets_log/types.md).

## Validation Rules

The coordinator must enforce:

- `GroupID` is non-empty.
- `Topic` is non-empty when required by the API.
- `MemberID` is non-empty after join.
- `__consumer_offsets` exists in the current metadata image before coordinator
  lookup.
- `__consumer_offsets` has at least one partition.
- The assigned `__consumer_offsets` partition has a known leader broker.
- The coordinator broker endpoint can be resolved from metadata.
- A broker only mutates group state for groups whose assigned
  `__consumer_offsets` partition it currently leads.
- Offset commits must use non-negative offsets.
- Fetch and offset commit requests must reference partitions assigned to the
  calling member.

## Failure Modes

The coordinator implementation should explicitly handle:

- missing or stale metadata image
- missing `__consumer_offsets` topic
- `__consumer_offsets` topic with zero partitions
- unknown leader for an assigned consumer offsets partition
- unknown broker endpoint for the computed leader
- contacted broker is not the coordinator
- append failure while writing group metadata
- append failure while writing member metadata
- append failure while writing offset commits
- coordinator leadership loss after lookup
- fetch requests for partitions not assigned to the member
- offset commit requests for partitions not assigned to the member

If the broker loses leadership for a `__consumer_offsets` partition, it must stop
serving group mutations for groups mapped to that partition and remove or mark
the affected in-memory group state as inactive.

## Testing Strategy

Unit tests should cover:

- deterministic `group.id` to `__consumer_offsets` partition mapping
- `FindCoordinator` returns local broker when local broker leads the assigned
  partition
- `FindCoordinator` returns the actual leader when local broker does not lead
  the assigned partition
- `FindCoordinator` appends `ConsumerGroupRegistrationRecord` for a newly
  materialized local group
- `FindCoordinator` does not append duplicate group metadata for an already
  materialized local group
- `FindCoordinator` updates in-memory group indexes after a successful append
- `FindCoordinator` rejects missing `__consumer_offsets` metadata
- `FindCoordinator` rejects `__consumer_offsets` with zero partitions
- `FindCoordinator` fails when the assigned partition leader cannot be resolved
- `JoinGroup` rejects requests sent to a non-coordinator broker
- `JoinGroup` materializes missing local group state when the consumer was
  redirected by a non-coordinator `FindCoordinator` response
- `JoinGroup` assigns partitions only from the requested topic
- `JoinGroup` appends member metadata before updating in-memory member state
- `Fetch` rejects unknown members
- `Fetch` rejects partitions not assigned to the member
- `Fetch` returns records whose offsets allow highest-fetched-offset tracking
- `OffsetCommit` rejects unknown members
- `OffsetCommit` rejects negative offsets
- `OffsetCommit` rejects partitions not assigned to the member
- `OffsetCommit` appends commit records before updating in-memory committed
  offsets
- leadership loss clears or inactivates local groups mapped to the lost
  `__consumer_offsets` partition
- replaying the consumer offsets log rebuilds group, member, and committed
  offset state

## Test Contracts

Until the exact group hash algorithm is defined, tests should depend only on an
injected hash function or a documented package-level helper. Tests must not rely
on incidental Go map iteration or runtime behavior for group-to-partition
assignment.

## Inconsistencies To Resolve

The following design points need resolution before implementation:

- The exact deterministic hash algorithm for `group.id` is unspecified. All
  brokers must use the same algorithm or coordinator lookup will split groups.
- Consumer `Fetch` being sent to the group coordinator is convenient for this
  phase, but normal Kafka fetches are served by partition leaders. This design
  must either define coordinator-side routing to partition leaders or later move
  data fetches to topic-partition leaders after assignment.
- `JoinGroup` needs a temporary partition assignment rule because rebalancing is
  out of scope. The chosen rule should be documented before implementation so
  tests can assert stable behavior.
- The response/error model for "not coordinator" should align with
  [errors.md](../protocol/errors.md) once protocol errors are implemented.
- Leadership changes for `__consumer_offsets` need a concrete recovery flow:
  when a broker becomes leader, it should replay the partition log; when it
  loses leadership, it should stop accepting mutations for mapped groups.

## Open Items

The following items are intentionally left for later definition:

- exact wire encoding for the coordinator APIs
- hash algorithm for group-to-partition mapping
- rebalance protocol and generation IDs
- heartbeat and session timeout behavior
- multi-topic group membership
- coordinator-side fetch routing versus direct partition-leader fetch
- committed offset fetch API for consumer restart recovery
