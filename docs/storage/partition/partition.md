# Partition Design

## Purpose

The partition component defines how a topic partition is identified and
described in cluster metadata.

In this phase, the partition document focuses on the metadata shape needed to
route produce and fetch requests correctly. It does not define the append-only
log internals, replication protocol, or reassignment workflow in detail.

## Directory Guide

- [api.md](./api.md): partition log-directory layout and lifecycle-facing APIs
- [types.md](./types.md): metadata model, identifiers, and recommended Go types
- [testing.md](./testing.md): validation rules and unit test expectations
- [segment.md](../log/segment.md): segment rollover rules used by partition log
  directories

## Goals

The partition design must provide:

- A stable metadata format for each partition
- A partition identifier that includes both topic and partition id
- A unique partition name derived from the topic name and partition id
- Leader broker metadata for request routing
- Replica broker metadata for replication placement
- ISR metadata for write availability and acknowledgement decisions
- A clear on-disk segment layout for each partition log directory

## Non-Goals

The initial partition design does not attempt to define:

- Partition reassignment steps
- Replica catch-up protocol details
- Consumer-group ownership or rebalancing behavior
- Raft-based metadata propagation

Those behaviors can be added after the core partition metadata model is
implemented.

## Responsibilities

The partition component is responsible for:

- Defining the metadata fields that describe a partition
- Defining how a partition is uniquely identified
- Exposing leader, replica, and ISR membership in memory
- Providing a metadata shape that brokers and clients can read consistently
- Defining the partition log-directory layout used by rolled segments

The partition component is not responsible for:

- Storing record bytes directly
- Implementing replica synchronization
- Electing leaders
- Performing partition reassignment

Validation rules and unit test expectations are tracked in
[testing.md](./testing.md).
