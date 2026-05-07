# Group Coordinator Design

## Purpose

The group coordinator is a broker responsibility for consumer group metadata.
It extends the general broker design by defining how a broker locates consumer
group coordinators, stores group membership, and persists committed offsets.

A group coordinator is not a separate process in this phase. It is the broker
that currently leads the `__consumer_offsets` partition assigned to a consumer
group. The assignment is deterministic:

```text
hash(group.id) % number_of___consumer_offsets_partitions
```

The resulting partition ID identifies the single `__consumer_offsets` partition
that owns the group's metadata and committed offsets.

## Directory Guide

- [api.md](./api.md): `FindCoordinator`, `JoinGroup`, consumer `Fetch`, and
  `OffsetCommit` request/response behavior
- [types.md](./types.md): coordinator state and recommended Go types
- [testing.md](./testing.md): validation, failure modes, testing strategy,
  inconsistencies, and open items
- [consumer_offsets_log.md](../metadata/consumer_offsets_log/consumer_offsets_log.md): durable
  `__consumer_offsets` record ownership, schema, replay, and compaction keys

## Relationship to Broker Design

The broker design in [broker.md](../broker/broker.md) defines node identity,
startup metadata discovery, listener metadata, and the in-memory view of
partitions led by the local broker. This document defines the consumer-group
specific behavior that runs on top of that broker runtime.

The group coordinator uses:

- the broker's current metadata image to discover `__consumer_offsets`
  partitions
- partition leader metadata to determine the coordinator broker for a group
- broker endpoint metadata to return coordinator host and port information
- the local leader-partition view to decide whether this broker is coordinator
  for a group
- the consumer offsets log to persist group metadata and committed offsets

## Goals

The group coordinator design must provide:

- deterministic mapping from `group.id` to a `__consumer_offsets` partition
- coordinator discovery through `FindCoordinator`
- consumer group membership through `JoinGroup`
- coordinator-owned consumer `Fetch` behavior for assigned partitions
- durable offset storage through `OffsetCommit`
- in-memory lookup of groups coordinated by the local broker
- clear redirect behavior when a contacted broker is not the coordinator

## Non-Goals

This phase does not define:

- heartbeats
- session timeouts
- group membership changes after startup
- rebalance rounds
- generation IDs
- static membership
- cooperative assignment
- multiple topics per group member
- manual offset commit mode from the CLI
- transactional offset commits
- full Kafka wire compatibility

Those can be added later without changing the core ownership rule that a group
is coordinated by the leader of its assigned `__consumer_offsets` partition.

## Coordinator Ownership Model

Each consumer group maps to exactly one `__consumer_offsets` partition.

Ownership lookup steps:

1. Read the current metadata image.
2. Find topic metadata for `__consumer_offsets`.
3. Read `PartitionCount` for `__consumer_offsets`.
4. Compute `partitionID = hash(group.id) % PartitionCount`.
5. Look up the leader broker ID for
   `(__consumer_offsets, partitionID)`.
6. Resolve the leader broker ID to broker endpoint metadata.
7. Treat that broker as the group coordinator.

If the contacted broker is the leader for the computed `__consumer_offsets`
partition, it returns itself. If it is not the leader, it returns the actual
leader broker for that partition.

The hash function must be deterministic across brokers and process restarts.
The exact hash algorithm should be defined before implementation begins.

## Consumer Offsets Log

Consumer group metadata and committed offsets are persisted through the
consumer offsets log for the assigned `__consumer_offsets` partition.

The log is conceptually the segmented append-only log for that internal
partition. It should follow the storage mechanics defined under
[docs/storage/log](../storage/log) and the internal-topic rules defined in
[topic.md](../storage/topic/topic.md).

The durable record types are defined in
[consumer_offsets_log.md](../metadata/consumer_offsets_log/consumer_offsets_log.md). The
coordinator's replayed in-memory structures are defined in [types.md](./types.md).

## Coordinator Request Flow

The consumer APIs are defined in [api.md](./api.md). At a high level:

1. The consumer sends `FindCoordinator` to a bootstrap broker.
2. The contacted broker computes the assigned `__consumer_offsets` partition.
3. The contacted broker returns the leader of that partition as coordinator.
4. The consumer sends `JoinGroup` to that coordinator.
5. The coordinator materializes group metadata if needed and assigns partitions.
6. The consumer sends `Fetch` and `OffsetCommit` to the coordinator in this
   phase.

## Leadership Changes

If a broker loses leadership for a `__consumer_offsets` partition, it must stop
serving group mutations for groups mapped to that partition and remove or mark
the affected in-memory group state as inactive.

On startup or leadership acquisition, the broker should rebuild local
coordinator state by replaying the relevant consumer offsets log partition.

Failure modes and testing expectations are defined in [testing.md](./testing.md).
