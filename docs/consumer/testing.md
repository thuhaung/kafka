# Consumer Testing

## Purpose

This document defines validation rules, failure modes, test expectations, and
known unresolved design issues for the consumer.

Coordinator-owned request behavior is defined in
[api.md](../group_coordinator/api.md).

## Validation Rules

The consumer CLI and startup path must enforce:

- `group.id` is required
- `topic` is required
- `bootstrap.servers` is required
- `auto.commit.interval.ms`, when supplied, must parse as a positive integer
- startup must fail if `FindCoordinator` cannot return coordinator broker info
- startup must fail if `JoinGroup` cannot return a member ID
- startup must fail if `JoinGroup` returns partition IDs that are invalid for
  the requested topic
- manual offset commit mode cannot be selected from the CLI in this phase

## Failure Modes

The consumer implementation should explicitly handle:

- missing required CLI arguments
- invalid `auto.commit.interval.ms`
- unreachable bootstrap broker
- `FindCoordinator` failure
- coordinator response missing node ID, host, or port
- unreachable group coordinator
- `JoinGroup` failure
- empty partition assignment
- `Fetch` failure during the polling loop
- malformed fetch response
- `OffsetCommit` failure

For polling-loop failures, the consumer should report the error clearly and
continue polling when possible. This phase does not support heartbeats, group
membership changes, or rebalancing.

## Testing Strategy

Consumer tests should cover:

- CLI validation rejects missing `group.id`
- CLI validation rejects missing `topic`
- CLI validation rejects missing `bootstrap.servers`
- CLI validation accepts optional `auto.commit.interval.ms`
- CLI validation rejects invalid auto-commit intervals
- startup sends `FindCoordinator` with `group.id`
- startup stores coordinator node ID, host, and port
- startup sends `JoinGroup` to the returned coordinator
- startup stores returned `member.id`
- startup stores assigned partition IDs
- offset commit mode defaults to automatic
- polling skips `Fetch` when no partitions are assigned
- polling sends `Fetch` with group ID, member ID, topic, and assigned partitions
- fetch responses update highest fetched offsets per partition
- automatic commit sends `OffsetCommit` when the interval has elapsed
- automatic commit includes offset, partition, topic, group ID, and member ID
- automatic commit does not send offsets for partitions with no fetched records

## Test Contracts

Startup tests should use injected bootstrap and coordinator clients so they can
verify request ordering, request fields, and stored runtime state without
depending on real network availability.

Polling tests should use deterministic clocks and injected ticker behavior so
automatic commit intervals can be asserted without relying on wall-clock time.

Fetch response tests should include empty partition responses and multi-record
partition responses so highest-fetched-offset tracking is verified directly from
record offsets.

## Open Items

The following items are intentionally left for later definition:

- exact transport encoding for the logical group coordinator APIs defined in
  [api.md](../group_coordinator/api.md)
- heartbeat and session timeout behavior
- group membership changes after startup
- rebalance behavior
- manual commit CLI support
- configurable polling interval
