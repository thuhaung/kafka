# Consumer Startup and Runtime Flow

## Startup Flow

Consumer startup follows this sequence:

1. Parse CLI arguments.
2. Validate that `group.id`, `topic`, and `bootstrap.servers` are present.
3. Validate `auto.commit.interval.ms` when provided.
4. Pick one configured bootstrap broker as the initial contact point.
5. Send a `FindCoordinator` API request using `group.id`.
6. Receive the group coordinator broker information from `FindCoordinator`.
7. Store the coordinator broker information in memory.
8. Send a `JoinGroup` API request to the coordinator using `group.id`.
9. The group coordinator applies its partition assignment strategy for the
   group.
10. Receive a `member.id` and assigned partition IDs from the coordinator.
11. Store the returned member ID and partition assignments in memory.
12. Start the polling loop.

The `FindCoordinator` API is specified in
[api.md](../group_coordinator/api.md). The response must identify the
coordinator broker with at least:

- node ID
- host
- port

## Runtime Access Model

After startup, the consumer should treat its parsed configuration and join
response as the local source of truth for this phase.

During the consumer's lifetime:

- all coordinator-directed requests use the broker returned by
  `FindCoordinator`
- `JoinGroup` must complete before the polling loop starts
- fetches use the partition assignment returned by `JoinGroup`
- highest fetched offsets are tracked in memory per partition
- automatic offset commits use the elapsed time since the last successful
  commit

The consumer should not start polling until `GroupCoordinator`, `GroupID`,
`MemberID`, `Topic`, and `AssignedPartitionIDs` have been initialized.

## Polling Loop

After startup completes, the consumer runs a polling loop every 30 seconds.
This interval is fixed in this phase.

Each loop iteration performs:

1. Check whether `AssignedPartitionIDs` is non-empty.
2. If no partitions are assigned, skip fetch and wait for the next poll tick.
3. If partitions are assigned, send `Fetch` to the group coordinator.
4. Include `group.id`, `member.id`, topic name, and assigned partition IDs in
   the `Fetch` request.
5. Process any returned messages.
6. Track the highest fetched offset per partition.
7. If `OffsetCommitMode` is `Automatic`, check whether
   `auto.commit.interval.ms` has elapsed since the last successful commit.
8. If the interval has elapsed, send `OffsetCommit` for fetched partition
   offsets.
9. Include offset, partition, topic name, `group.id`, and `member.id` in each
   logical offset commit.

Automatic offset commits should only include partitions for which the consumer
has fetched at least one offset.

## Startup Failure Handling

Startup should fail before polling begins when:

- required CLI settings are missing
- `auto.commit.interval.ms` is invalid
- no bootstrap broker can be contacted
- `FindCoordinator` cannot return coordinator broker information
- the coordinator response is missing node ID, host, or port
- the returned group coordinator cannot be reached
- `JoinGroup` cannot return a member ID
- `JoinGroup` returns partition IDs that are invalid for the requested topic

For polling-loop failures, the consumer should report the error clearly and
continue polling when possible. This phase does not support heartbeats, group
membership changes, or rebalancing.
