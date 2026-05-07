# Broker Design

## Purpose

The broker component defines the node-level runtime identity and listener
metadata required for a process to join the cluster and communicate over the
network.

In this first phase, the broker design is limited to configuration loading,
startup metadata discovery, and in-memory metadata access. Produce, fetch,
replication, and other broker APIs are intentionally deferred.

## Directory Guide

- [types.md](./types.md): node metadata, server properties, Go types, and
  validation rules
- [api.md](./api.md): broker package interfaces and consumer-facing broker API
  placeholders
- [startup.md](./startup.md): startup flow, controller discovery, runtime
  access, leader partition state, and startup failure handling
- [testing.md](./testing.md): validation, failure modes, testing strategy, and
  open items

## Goals

The broker design must provide:

- A stable metadata format for each node
- A server properties file shape loaded from the `config.path` passed to
  `kafka-server-start.sh`
- Startup behavior that reads the configured properties file and maps it into
  Go types
- An in-memory broker configuration object used for the lifetime of the process
- Listener metadata for broker-client, broker-broker, and broker-controller
  communication
- A startup API that performs controller quorum discovery
- Controller quorum configuration that a broker can use to choose one
  controller contact point
- A startup fetch of cluster metadata by broker-role nodes using the configured
  cluster ID
- An in-memory view of the partitions for which this broker is currently the
  leader

## Non-Goals

The initial broker design does not attempt to define:

- Produce APIs
- Fetch APIs
- Replication RPCs
- Partition leadership election or reassignment decisions
- Dynamic broker reconfiguration while the process is running
- Authentication or encrypted transport
- Custom listener names

Those APIs and behaviors should be specified later instead of being guessed now.

## Responsibilities

The broker component is responsible for:

- Loading node metadata from the properties file supplied by `config.path` at
  startup
- Validating that the configured metadata is internally consistent
- Exposing that metadata as a Go object in memory
- Providing listener information to the rest of the process during runtime
- Exposing whether the process acts as a broker or a controller
- Providing the local log directory path for partition logs and the local copy
  of the cluster metadata log
- Allowing a broker-role node to choose a random controller from the configured
  controller quorum
- Handling controller-leader discovery during startup
- Fetching cluster metadata during startup by using the cluster ID
- Maintaining a derived in-memory view of partitions whose `LeaderBrokerID`
  matches the local `NodeID`

The broker component is not responsible for:

- Defining produce or fetch request formats
- Defining replication behavior
- Defining controller-to-controller quorum membership
- Persisting dynamic cluster metadata updates
- Deciding which broker should become leader for a partition

Validation rules, failure modes, unit test expectations, and unresolved design
items are tracked in [testing.md](./testing.md).
