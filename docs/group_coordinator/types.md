# Group Coordinator Types

## Purpose

This document defines the recommended in-memory types used by the group
coordinator.

Durable `__consumer_offsets` metadata records, including
`ConsumerGroupRegistrationRecord`, `ConsumerRegistrationRecord`, and
`ConsumerOffsetCommitRecord`, are defined in
[types.md](../metadata/consumer_offsets_log/types.md).

## In-Memory State

The local coordinator keeps an in-memory index of groups for which this broker
currently leads the assigned `__consumer_offsets` partition.

Recommended state:

```go
package groupcoordinator

import "time"

type BrokerInfo struct {
	NodeID int32
	Host   string
	Port   int32
}

type GroupState struct {
	GroupID                    string
	ConsumerOffsetsPartitionID int32
	Coordinator                BrokerInfo
	Members                    map[string]MemberState
	CommittedOffsets           map[OffsetKey]int64
	CreatedAt                  time.Time
}

type MemberState struct {
	MemberID             string
	Topic                string
	AssignedPartitionIDs []int32
	JoinedAt             time.Time
}

type OffsetKey struct {
	GroupID     string
	Topic       string
	PartitionID int32
}

type CoordinatorState struct {
	Groups map[string]GroupState
}
```

Recommended read API:

```go
type StateReader interface {
	Group(groupID string) (GroupState, error)
}
```

Implementation notes:

- `Groups` is keyed by group ID and contains only groups coordinated by the
  local broker.
- `Group(groupID)` returns the current state for one group and should return a
  stable not-found error when the group is unknown.
- Public state accessors must return defensive copies.
- On startup or leadership acquisition, the broker should rebuild this state by
  replaying the relevant consumer offsets log partition.
- A reverse index from `__consumer_offsets` partition ID to group IDs is not
  required in this phase. It can be added later when rebalancing or explicit
  leadership-transfer cleanup needs it.
