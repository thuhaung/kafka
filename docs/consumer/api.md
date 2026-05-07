# Consumer APIs

## Purpose

This document defines the consumer CLI contract and the broker APIs the
consumer calls during startup, polling, and automatic offset commit.

The consumer depends on the group coordinator APIs defined in
[api.md](../group_coordinator/api.md). The exact transport encoding is deferred
to protocol design.

## CLI Contract

The consumer CLI is exposed as `kafka-console-consumers`.

Required CLI settings:

| Setting | Required | Description |
| --- | --- | --- |
| `group.id` | Yes | Consumer group identifier. The CLI must return an error when omitted. |
| `topic` | Yes | Topic name to consume from. |
| `bootstrap.servers` | Yes | One or more broker endpoints used for the initial coordinator lookup. |

Optional CLI settings:

| Setting | Required | Default | Description |
| --- | --- | --- | --- |
| `auto.commit.interval.ms` | No | Implementation-defined | Minimum interval between automatic offset commit requests. |

For this phase, `offset.commit.mode` is always `Automatic`. The CLI must not
allow users to configure manual commit mode yet.

Example:

```shell
kafka-console-consumers \
  --bootstrap.servers localhost:9092 \
  --topic orders \
  --group.id orders-reader \
  --auto.commit.interval.ms 5000
```

Invalid example:

```shell
kafka-console-consumers \
  --bootstrap.servers localhost:9092 \
  --topic orders
```

This command must fail because `group.id` is missing.

## Broker APIs Used By Consumers

The consumer depends on the group coordinator APIs defined in
[api.md](../group_coordinator/api.md):

| API | Called By | Sent To | Purpose |
| --- | --- | --- | --- |
| `FindCoordinator` | consumer startup | bootstrap broker | Locate the broker assigned as coordinator for `group.id`. |
| `JoinGroup` | consumer startup | group coordinator | Join the consumer group; the coordinator assigns partitions and returns member identity plus assignments. |
| `Fetch` | polling loop | group coordinator | Fetch new messages for the consumer's assigned partitions. |
| `OffsetCommit` | polling loop | group coordinator | Commit the highest fetched offsets for assigned partitions. |

The consumer must treat the coordinator returned by `FindCoordinator` as the
target for `JoinGroup`, `Fetch`, and `OffsetCommit` in this phase.

## Consumer Request Requirements

The `FindCoordinator` request must include:

| Field | Required | Description |
| --- | --- | --- |
| `group.id` | Yes | Consumer group identifier used to locate the coordinator. |

The `JoinGroup` request must include:

| Field | Required | Description |
| --- | --- | --- |
| `group.id` | Yes | Consumer group identifier. |
| `topic` | Yes | Topic the consumer wants to consume. |

The polling-loop `Fetch` request must include:

| Field | Required | Description |
| --- | --- | --- |
| `group.id` | Yes | Consumer group identifier. |
| `member.id` | Yes | Coordinator-assigned member identifier. |
| `topic` | Yes | Topic being consumed. |
| assigned partition IDs | Yes | Partitions returned by `JoinGroup`. |

Each logical `OffsetCommit` must include:

| Field | Required | Description |
| --- | --- | --- |
| `group.id` | Yes | Consumer group identifier. |
| `member.id` | Yes | Coordinator-assigned member identifier. |
| `topic` | Yes | Topic being consumed. |
| partition ID | Yes | Partition whose offset is being committed. |
| offset | Yes | Highest fetched offset for the partition. |

Automatic offset commits should only include partitions for which the consumer
has fetched at least one offset.
