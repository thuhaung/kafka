# Consumer Types

## Purpose

This document defines the recommended in-memory types used by the consumer
runtime.

The durable group metadata and committed offset records are owned by the group
coordinator and are defined in
[types.md](../metadata/consumer_offsets_log/types.md).

## In-Memory State

The consumer must keep an in-memory structure close to:

```go
package consumer

type OffsetCommitMode string

const (
	OffsetCommitAutomatic OffsetCommitMode = "automatic"
	OffsetCommitManual    OffsetCommitMode = "manual"
)

type BrokerInfo struct {
	NodeID int
	Host   string
	Port   int
}

type Consumer struct {
	GroupCoordinator     BrokerInfo
	GroupID              string
	MemberID             string
	Topic                string
	AssignedPartitionIDs []int
	AutoCommitIntervalMS int
	OffsetCommitMode     OffsetCommitMode
	HighestFetchedOffset map[int]int64
}
```

Implementation notes:

- `OffsetCommitMode` must default to `OffsetCommitAutomatic`.
- `OffsetCommitManual` is defined for the state model but is not configurable
  from the CLI in this phase.
- `HighestFetchedOffset` is keyed by partition ID.
- The consumer should update `HighestFetchedOffset` whenever a fetch response
  returns records for an assigned partition.
- The consumer should not start polling until `GroupCoordinator`, `GroupID`,
  `MemberID`, `Topic`, and `AssignedPartitionIDs` have been initialized.

## State Ownership

Consumer state is process-local and durable only for the lifetime of the CLI
process in this phase.

The consumer owns:

- coordinator endpoint metadata returned by `FindCoordinator`
- group ID and requested topic from CLI settings
- member ID returned by `JoinGroup`
- assigned partition IDs returned by `JoinGroup`
- configured automatic commit interval
- offset commit mode
- highest fetched offset per assigned partition

The consumer does not own:

- partition assignment decisions
- group membership metadata
- durable committed offsets
- consumer offsets log replay
- rebalance or heartbeat state
