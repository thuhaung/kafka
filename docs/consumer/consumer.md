# Consumer Design

## Purpose

The consumer component defines the client-side process that reads messages from
topic partitions through broker APIs.

In this phase, the project only supports consumers that are members of a
consumer group. Standalone consumers are intentionally unsupported. A consumer
started from the `kafka-console-consumers` CLI without a `group.id` must fail
before making any broker request.

## Directory Guide

- [api.md](./api.md): CLI contract and broker APIs used by consumers
- [startup.md](./startup.md): startup flow, coordinator discovery, polling loop,
  and startup failure handling
- [types.md](./types.md): consumer runtime state and recommended Go types
- [testing.md](./testing.md): validation rules, failure modes, testing strategy,
  and open items

## Goals

The consumer design must provide:

- CLI validation for group-based consumption
- startup discovery of the consumer group coordinator
- consumer group join behavior
- in-memory consumer state for group membership and partition assignments
- a polling loop that fetches messages for assigned partitions
- automatic offset commit behavior based on a configurable interval

## Non-Goals

This phase does not define:

- standalone consumers outside a consumer group
- manual offset commit configuration from the CLI
- custom polling intervals
- client-side partition assignment strategies
- heartbeats
- group membership changes after startup
- rebalancing
- durable local consumer state

Partition assignment is owned by the group coordinator when the consumer joins
the group. Heartbeats, membership changes, and rebalancing should be added in
later consumer group phases.

## Responsibilities

The consumer component is responsible for:

- parsing and validating CLI settings
- locating the group coordinator through `FindCoordinator`
- joining the consumer group through `JoinGroup`
- keeping coordinator, member, assignment, and offset-tracking state in memory
- polling assigned partitions through the coordinator-facing `Fetch` API in
  this phase
- committing fetched offsets automatically when the configured interval elapses

The consumer component is not responsible for:

- assigning partitions to group members
- persisting group metadata or committed offsets
- serving fetch requests from partition logs
- detecting membership changes after startup
- running the rebalance protocol

Coordinator-owned request behavior is defined in
[api.md](../group_coordinator/api.md). Consumer validation rules, failure modes,
unit test expectations, and unresolved design items are tracked in
[testing.md](./testing.md).
