# Partition Types

## Purpose

This document defines the partition metadata model, identification rules, and
recommended Go types for `internal/partition`.

## Partition Metadata Model

Each partition metadata record should include the following fields:

| Field | Type | Required | Description |
| --- | --- | --- | --- |
| TopicName | string | Yes | Name of the topic that owns the partition |
| PartitionID | int32 | Yes | Numeric partition id within the topic |
| PartitionName | string | Yes | Unique partition name in the form `<topic-name>-<partition-id>`, for example `orders-0` |
| LeaderBrokerNodeID | int | Yes | Node id of the broker currently acting as the partition leader |
| ReplicaBrokerNodeIDs | []int | Yes | Node ids of all brokers assigned to host replicas for the partition |
| ISRNodeIDs | []int | Yes | Node ids of the in-sync replicas currently eligible for acknowledgement decisions |

This satisfies the required partition format:

- Topic name
- Partition id
- Partition name = topic name-partition id, for example `orders-0`
- Leader broker node id
- Replica broker node ids
- ISR node ids

## Identification Rules

Partitions are scoped by topic, so `PartitionID` is unique only within a single
topic.

To create a cluster-wide unique identifier, the partition metadata must also
store `PartitionName`, built as:

```text
<topic-name>-<partition-id>
```

Examples:

- `orders-0`
- `orders-1`
- `payments-0`

## Proposed Go Types

The exact package path may evolve, but `internal/partition` should expose types
close to:

```go
package partition

type Metadata struct {
	TopicName            string
	PartitionID          int32
	PartitionName        string
	LeaderBrokerNodeID   int
	ReplicaBrokerNodeIDs []int
	ISRNodeIDs           []int
}
```

Implementation notes:

- `PartitionName` should be derived from `TopicName` and `PartitionID` rather
  than entered independently by callers.
- `LeaderBrokerNodeID` should always be a member of `ReplicaBrokerNodeIDs`.
- `ISRNodeIDs` should always be a subset of `ReplicaBrokerNodeIDs`.
- `ReplicaBrokerNodeIDs` should not contain duplicates.
- `ISRNodeIDs` should not contain duplicates.

## Example

Example partition metadata for topic `orders`:

```text
Topic name: orders
Partition id: 0
Partition name: orders-0
Leader broker node id: 2
Replica broker node ids: [2, 4, 5]
ISR node ids: [2, 4]
```
