# Topic Design

## Purpose

The topic component defines how a topic is created, identified, configured, and
submitted to the cluster as metadata.

In this project, topic metadata is a control-plane concern rather than a
partition-log concern. The topic CLI and topic create API are responsible for
building a valid request, sending it to the cluster, following any
broker-to-controller redirect, and returning success or error to the caller.
Controller-side storage mechanics for topic metadata are defined in
[quorum.md](../../quorum/quorum.md).

## Directory Guide

- [api.md](./api.md): CLI surface, internal service API, create request flow,
  redirect semantics, and package placement
- [types.md](./types.md): topic metadata, configuration, internal topic
  lifecycle, and recommended Go types
- [testing.md](./testing.md): validation rules, failure modes, testing focus,
  and deferred topics
- [quorum.md](../../quorum/quorum.md): controller-side topic metadata
  storage mechanics

## Goals

The topic design must provide:

- A CLI program under `cmd` for topic operations
- An API for creating new topics
- A bootstrap-broker config that accepts a single broker address
- Support for the following creation-time topic configs:
  - `partitions`
  - `replication.factor`
  - retention parameters
  - `min.insync.replicas`
- Default `partitions` to `1` when not explicitly provided
- A redirect-aware create flow from broker to controller leader
- Validation rules for topic names and topic config values
- A metadata shape that brokers can use to create and host partitions later

## Non-Goals

The initial topic design does not attempt to define:

- Partition reassignment
- Dynamic config changes after create
- Topic deletion tombstones and cleanup behavior
- ACLs or authentication around topic administration
- Full controller failover mechanics beyond relying on the quorum metadata log

Those can be layered on after the basic topic-creation path is working.

## Responsibilities

The topic component is responsible for:

- Accepting topic-create requests from the CLI or internal callers
- Validating topic names and configuration values
- Packaging topic-create requests for network transport
- Handling broker redirect responses during topic creation
- Exposing a stable in-memory representation of topic metadata

The topic component is not responsible for:

- Storing user messages
- Serving fetch or produce traffic
- Defining controller metadata-log internals
- Replicating partition log entries

Validation rules, failure modes, unit test expectations, and unresolved design
items are tracked in [testing.md](./testing.md).
