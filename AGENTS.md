# AGENTS.md

## Purpose

This repository is a Go-based Kafka clone focused on learning and implementing the core distributed log and broker mechanics behind Apache Kafka, with Raft-based leader election.

This document defines the implementation scope, component boundaries, directory responsibilities, and delivery expectations for all contributors and coding agents working in this repository.

## Project Goals

The system must support the following functional requirements:

### 1. Append-Only Log

The log subsystem is the foundation of the project and must provide:

- Offset-based addressing for records
- Segmented storage
- Retention policies
- Log compaction

### 2. Partitioning

- Topics are split into partitions
- Each partition owns an independent append-only log
- Producers and consumers operate against partitions through broker APIs

### 3. Topic Management

- Topics can be created through a CLI program under [`cmd`](./cmd)
- Topics can be configured through a CLI program under [`cmd`](./cmd)
- Topics can be read through a CLI program under [`cmd`](./cmd)
- Topics can be deleted through a CLI program under [`cmd`](./cmd)

### 4. Producer and Consumer Basics

- Producers can push records to brokers
- Producers automatically retry on retriable failures
- Consumers can poll records
- Consumers can commit offsets

### 5. Broker Responsibilities

- Validate incoming messages
- Append accepted records to the correct partition log
- Replicate messages to followers

### 6. Rebalancing

- Re-assign partitions across brokers and/or consumers when membership changes

### 7. Leader Election

- Use Raft for broker leader election and metadata coordination

### 8. Failure Handling

The implementation must explicitly handle:

- Broker crashes
- Consumer crashes
- Follower lag

## Core Components

The minimum core components of the system are:

- `Log`: append-only, segmented, retained, compacted storage
- `Message`: record representation, validation schema, serialization, and metadata
- `Topic`: topic lifecycle management, configuration, and metadata access
- `Broker`: request handling, validation, replication, partition hosting
- `Producer`: client-side send and retry logic
- `Consumer`: polling and offset commit logic

Additional supporting components are expected, including but not limited to:

- `Log Cleaner`: enforces retention and compaction policies
- `Partition Manager`: tracks partition placement and assignment
- `Replication Manager`: handles follower sync and lag tracking
- `Raft Coordinator`: manages leader election and distributed metadata state
- `Rebalancer`: reacts to membership changes and moves partition ownership

## Repository Layout

The repository is organized as follows:

- [`cmd`](./cmd): CLI programs for client-facing APIs
- [`internal`](./internal): application internals and reusable implementation packages
- [`docs`](./docs): component-level design documents
- [`skills`](./skills): project skills, workflows, or agent support material

### Directory Rules

- All implementation code must be written in Go
- Topic operations must be exposed as a CLI program under `cmd`
- Producer and consumer APIs must be exposed as CLI programs under `cmd`
- Shared logic, transport, storage, coordination, and broker internals must live under `internal`
- Each major component should have a design document in `docs` before or alongside implementation
- Skills or agent-specific instructions belong in `skills`

## Recommended Internal Package Structure

The exact package layout may evolve, but contributors should prefer a structure close to:

- `internal/storage/log`
- `internal/message`
- `internal/topic`
- `internal/broker`
- `internal/producer`
- `internal/consumer`
- `internal/partition`
- `internal/replication`
- `internal/raft`
- `internal/rebalancer`
- `internal/cleaner`
- `internal/protocol`
- `internal/tests`

The goal is clear ownership and low coupling between storage, coordination, networking, and client behavior.

## Implementation Order

To keep the project incremental and testable, work should proceed in roughly this order:

1. Log primitives
2. Topic abstraction and topic-management CLI
3. Partition abstraction
4. Broker append and fetch paths
5. Producer CLI and retry behavior
6. Consumer CLI, polling, and offset commit
7. Replication between leader and followers
8. Retention and compaction via log cleaner
9. Rebalancing logic
10. Raft-based leader election
11. Failure recovery and resilience scenarios

## Component Expectations

### Log

The log component should define:

- Record format
- Offset allocation
- Segment rolling rules
- Read path by offset
- Retention policy enforcement
- Compaction behavior by key where applicable

### Message

The message component should define:

- Record structure and field layout
- Keys, values, headers, timestamps, and offsets
- Validation rules for broker ingestion
- Serialization and deserialization behavior
- Message size and format constraints
- Metadata required for replication and consumer delivery

### Topic

The topic component should define:

- Topic creation behavior
- Topic configuration behavior
- Topic metadata read behavior
- Topic deletion behavior
- Validation rules for topic names and configuration values
- How topic metadata maps to partitions and brokers
- The CLI surface exposed under `cmd`

### Broker

The broker should define:

- Message validation rules
- Topic/partition routing
- Produce and fetch APIs
- Replication behavior
- Leader/follower responsibilities
- Failure and recovery behavior

### Producer

The producer CLI should support:

- Sending records to a topic and partition or partitioning strategy
- Retry behavior for transient failures
- Clear error reporting

### Consumer

The consumer CLI should support:

- Polling records
- Tracking consumer position
- Committing offsets
- Recovering from committed offsets after restart

### Topic CLI

The topic CLI should support:

- Creating topics
- Reading topic metadata and configuration
- Updating topic configuration
- Deleting topics
- Clear error reporting for invalid topic requests

### Rebalancing

The system should include:

- Membership change detection
- Partition re-assignment logic
- Safe ownership transfer

### Raft

The Raft component should be responsible for:

- Leader election
- Term tracking
- Log or metadata agreement needed for broker coordination

## Testing Requirements

Testing is mandatory after every implementation step.

### Rules

- Every new feature must include unit tests
- Tests should be added in the same change as the implementation
- No component should be considered complete without passing unit tests
- Failure scenarios should be tested where practical, not only happy paths

### Minimum Testing Focus

- Log append/read/segment/retention/compaction behavior
- Topic creation/configuration/read/deletion behavior
- Producer retry behavior
- Consumer polling and offset commit behavior
- Broker validation and replication logic
- Rebalancing correctness
- Raft leader election behavior
- Crash and lag handling scenarios

## Documentation Requirements

For each major component, contributors should maintain a design note in `docs` covering:

- Responsibilities
- Public interfaces
- State model
- Failure modes
- Testing strategy

Suggested design docs include:

- `docs/storage/log/log.md`
- `docs/message.md`
- `docs/topic.md`
- `docs/broker.md`
- `docs/producer.md`
- `docs/consumer.md`
- `docs/replication.md`
- `docs/rebalancing.md`
- `docs/raft.md`
- `docs/failure-handling.md`

## Contribution Guidelines

When implementing work in this repository:

- Keep changes incremental
- Prefer small, testable packages
- Separate transport concerns from domain logic
- Avoid leaking CLI concerns into `internal`
- Document assumptions in `docs` when behavior is non-trivial
- Add or update tests immediately after implementing behavior

## Definition of Done

A task is considered complete only when:

- The implementation is in Go
- The code is placed in the correct directory
- Relevant unit tests are added
- Tests pass locally
- The corresponding design documentation is created or updated

## Operating Principle

This project should be built as an educational but disciplined system: start from a solid log abstraction, layer broker and client behavior on top, and only then move into coordination, replication, and failure recovery.
