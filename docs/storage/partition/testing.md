# Partition Testing

## Purpose

This document defines validation rules and test expectations for the partition
metadata and directory-layout component.

## Validation Rules

Partition metadata should satisfy the following initial rules:

- `TopicName` must be a valid topic name as defined in
  [topic.md](../topic/topic.md).
- `PartitionID` must be greater than or equal to `0`.
- `PartitionName` must equal `<TopicName>-<PartitionID>`.
- `LeaderBrokerNodeID` must appear in `ReplicaBrokerNodeIDs`.
- `ReplicaBrokerNodeIDs` must contain at least one broker node id.
- `ISRNodeIDs` must contain only broker node ids already present in
  `ReplicaBrokerNodeIDs`.

## Testing Strategy

The partition component should include unit tests covering:

- Partition-name derivation from topic name and partition id
- Validation that the leader is included in the replica set
- Validation that ISR members are a subset of replicas
- Rejection of duplicate replica or ISR node ids
- Rejection of negative partition ids
- Creation of one `.log`, one `.index`, and one `.timeindex` file per segment
- Addition of a new segment file trio to the partition log directory after
  rollover
