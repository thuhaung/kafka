# Controller Quorum Design

## Purpose

The controller quorum component defines the controller-specific runtime behavior,
persisted state, metadata-image responsibilities, and startup discovery behavior
required for a process to act as a cluster controller.

This document extends the shared node structure already defined in
[broker.md](../broker/broker.md). Shared configuration fields, listener shapes,
property parsing rules, and common Go type structure should be read from that
document instead of being repeated here.

In this first phase, the controller quorum design covers configuration loading,
metadata-image construction, static leader designation, metadata-log location,
broker startup metadata fetch, and non-leader controller startup recovery.
Raft-based leader election is intentionally deferred.

## Directory Guide

- [api.md](./api.md): controller quorum request and response schemas, redirect
  behavior, retry semantics, leader handling, and wire encoding
- [types.md](./types.md): controller metadata model, server properties, field
  mapping, Go types, runtime access model, and metadata image state
- [testing.md](./testing.md): validation rules, failure modes, testing strategy,
  test contracts, and open items
- [controller.md](./controller.md): compatibility entry point for older links

## Goals

The controller quorum design must provide:

- Controller-specific additions to the shared node metadata defined in
  [broker.md](../broker/broker.md)
- A properties file shape loaded from the `config.path` passed to
  `kafka-server-start.sh`
- Startup behavior that reads the configured properties file and maps it into
  Go types
- An in-memory controller configuration object used for the lifetime of the
  process
- A persisted `is.leader` key mapped to `IsLeaderController`
- A persisted `metadata.log.dir` key that points to the controller metadata log
  location
- Listener metadata for controller communication
- A temporary non-Raft leader model driven by persisted configuration
- A startup metadata-fetch flow for non-leader controllers that matches the
  broker discovery flow
- Construction and publication of an in-memory cluster metadata image from the
  cluster metadata log
- Handling broker startup registration and metadata fetch requests at the leader
  controller

## Non-Goals

The initial controller quorum design does not attempt to define:

- Raft leader election
- Controller quorum voting behavior
- Metadata-log replication transport between controllers
- Topic-create request/response schemas
- Partition reassignment workflows
- Dynamic controller reconfiguration while the process is running
- Authentication or encrypted transport
- Full broker heartbeat or liveness semantics after initial broker registration

Those behaviors should be specified later instead of being guessed now.

## Responsibilities

The controller quorum component is responsible for:

- Loading controller metadata from the properties file supplied by `config.path`
  at startup
- Validating that the configured metadata is internally consistent
- Exposing that metadata as a Go object in memory
- Exposing whether the process is marked as the leader controller
- Providing the metadata log directory path for controller state
- Providing listener information used for broker-to-controller communication
- Serving as the temporary source of controller leadership information until
  Raft exists
- Allowing a non-leader controller to discover the current leader controller
  during startup
- Fetching cluster metadata during startup for non-leader controller recovery
- Replaying committed metadata-log records into the in-memory metadata image
- Applying new leader-controller state changes to the metadata image after they
  are appended to the metadata log
- Using an abstract metadata-log append/commit interface in this phase, until
  the concrete log component is implemented
- Serving the current metadata image to brokers that register and fetch cluster
  metadata during startup
- Returning shared protocol errors defined in [errors.md](../protocol/errors.md)
  for controller redirects and terminal request failures

The controller quorum component is not responsible for:

- Running Raft leader election
- Discovering leadership dynamically from quorum consensus
- Defining broker produce or fetch APIs
- Redefining the shared node, listener, or broker metadata structures already
  covered by [broker.md](../broker/broker.md) and
  [cluster_metadata.md](../metadata/cluster_metadata/cluster_metadata.md)

Validation rules, failure modes, unit test expectations, and unresolved design
items are tracked in [testing.md](./testing.md).

## Startup Flow

Controller startup should follow this sequence:

1. Start through the `kafka-server-start.sh` CLI program.
2. Parse CLI arguments.
3. Require exactly one `config.path` argument.
4. Reject unknown CLI arguments.
5. Read the server properties file from `config.path`.
6. Parse key-value properties.
7. Convert parsed values into a `NodeConfig`.
8. Validate required fields and listener constraints.
9. Keep the resulting `NodeConfig` in memory for the lifetime of the process.
10. Initialize controller runtime state using `MetadataLogDir`.
11. Build the local metadata image for `ClusterID`, or publish an empty image
   with `AppliedOffset = -1` if no metadata records exist yet.
12. If `IsLeaderController` is false, invoke the same startup discovery flow used
   by brokers.
13. For a non-leader controller, pick a random controller from
    `ControllerQuorum`, excluding its own controller endpoint.
14. For a non-leader controller, send `FetchMetadataImage` using `ClusterID`.
15. If the contacted controller returns `NOT_LEADER` with the current leader
    node ID and leader address, store that leader in memory as
    `LeaderController`.
16. If the contacted controller returns `NOT_LEADER` without a leader endpoint,
    treat it like `LEADER_UNKNOWN`.
17. If the contacted controller returns `LEADER_UNKNOWN`, `NOT_LEADER` without a
    leader endpoint, or cannot be reached, try another controller from
    `ControllerQuorum`.
18. Retry `FetchMetadataImage` against `LeaderController` when leader
    information becomes available.
19. Use the fetched metadata image to refresh local controller state for crash
    recovery.
20. Begin serving controller requests.
21. Expose leader status from `IsLeaderController`.
22. Use the in-memory `NodeConfig` whenever the process needs its local
    controller metadata.

The controller-role startup order is:

1. Parse CLI args.
2. Parse config.
3. Validate config.
4. Initialize metadata store and publish an empty image when needed.
5. If non-leader, fetch metadata from the leader.
6. Begin serving controller requests.

The process should not repeatedly reread the file during normal operation. If a
controller crashes and restarts, the operator starts it again with
`kafka-server-start.sh config.path=<path>`, and the controller loads the same
configuration path again during the new process startup.

Startup orchestration must be single-threaded until controller startup
completes. The controller must not begin serving requests until config
validation, metadata store initialization, and any required non-leader metadata
fetch have completed. Runtime TCP request handling may use the protocol server
model after startup enters the serving state.

The default startup communication timeout is `10s`. This timeout applies to TCP
connect, request frame write, response frame read, and each full startup API
call. Non-leader controller discovery must not retry forever.

## Startup Logging

Minimum startup logs should include:

- config loaded successfully, including role, node ID, and cluster ID
- selected controller endpoint for non-leader discovery, including controller
  node ID, host, and port
- metadata image built successfully, including cluster ID and applied offset
- fatal startup failures before the process exits or returns an error

## Immediate Abort Conditions

Controller startup must abort immediately and must not continue when any of
these conditions occur:

- missing `config.path`
- duplicate `config.path`
- unknown CLI argument
- unreadable file at `config.path`
- malformed property lines
- non-controller role configured for the controller process
- multiple configured roles in `process.roles`
- missing required listener
- invalid cluster ID
- invalid `controller.quorum.voters` formatting
- empty controller quorum
- invalid `is.leader` formatting
- missing or invalid `metadata.log.dir`
- metadata store or image initialization failure
- controller listener bind failure
- non-leader metadata fetch failure after the startup timeout
- terminal protocol error from the contacted controller
- fetched metadata image cluster ID mismatch

## Temporary Leader Model

Until Raft is implemented, controller leadership is a static configuration
concern.

The first-phase behavior is:

- Each controller reads `is.leader` from the properties file supplied by
  `config.path`.
- A controller with `IsLeaderController = true` behaves as the current leader
  controller.
- A controller with `IsLeaderController = false` behaves as a non-leader
  controller.
- A non-leader controller performs the same startup metadata-fetch flow used by
  brokers.
- During that flow, a non-leader controller stores the resolved leader in
  `LeaderController` in memory.
- A non-leader controller excludes its own endpoint when choosing a controller
  quorum member for startup discovery.
- If another non-leader controller does not yet know who the leader is, it may
  return `LEADER_UNKNOWN`.
- Non-leader controllers may respond to broker requests with `NOT_LEADER` and
  the leader controller information when that behavior is defined by
  higher-level APIs.

This model is intentionally temporary. It exists only to unblock metadata flows
before controller quorum consensus is implemented.

## Fetching Cluster Metadata

When a broker performs startup discovery as defined in
[broker.md](../broker/broker.md), it should both register itself with the
controller quorum and fetch the current metadata image for its `ClusterID`.

The recommended flow is:

1. The broker loads its local `NodeConfig`, including `ClusterID`, `NodeID`,
   its configured listener, and controller quorum.
2. The broker chooses a controller endpoint from `ControllerQuorum`.
3. The broker sends a startup metadata request that includes its cluster ID,
   broker ID, and broker contact metadata.
4. If the contacted controller is not the leader, it returns `NOT_LEADER` with
   the leader controller node ID and address when known.
5. If the contacted controller does not know the leader, it returns
   `LEADER_UNKNOWN`.
6. The broker retries against the returned leader or another quorum endpoint
   using the redirect behavior already defined in [broker.md](../broker/broker.md).
7. The leader controller validates that the broker's `ClusterID` matches a known
   image key.
8. The leader controller checks the current in-memory image for that cluster ID.
9. If the image already contains the broker's node ID and the stored broker
   metadata matches the request, the controller treats the request as a broker
   restart and returns the current image without appending another registration
   record.
10. If the image already contains the broker's node ID but the stored broker
    metadata conflicts with the request, the controller rejects the request with
    `INVALID_BROKER_REGISTRATION`.
11. If the image does not contain the broker's node ID, the leader controller
    appends a broker registration record to the cluster
   metadata log through an abstract append/commit interface. The record is
   `BrokerRegistrationRecord`, defined in
   [cluster_metadata_log.md](../metadata/cluster_metadata_log/cluster_metadata_log.md),
   and its payload includes the broker ID, cluster ID, and broker contact
   metadata required by `BrokerMetadata` in
   [cluster_metadata.md](../metadata/cluster_metadata/cluster_metadata.md).
12. After the registration record is committed, the leader controller applies it
    to the in-memory image for that cluster ID, adding the broker entry to the
    image's broker map.
13. The leader controller returns the current metadata image for that cluster ID
    to the broker.
14. The broker builds or replaces its own in-memory metadata image from the
    returned image.

This response is how the broker learns:

- the remaining brokers in the cluster
- broker endpoint metadata used for routing and replication
- topic and partition metadata
- which broker is leader for each partition

The response payload should be shaped as a metadata image or image delta rather
than as a controller-specific schema. That keeps broker and controller reads
aligned with [cluster_metadata.md](../metadata/cluster_metadata/cluster_metadata.md).

The concrete broker startup request and response schema is
`RegisterBrokerAndFetchMetadata`, defined in [api.md](./api.md). The read-only
controller startup metadata fetch is `FetchMetadataImage`, also defined in
[api.md](./api.md). `NOT_LEADER` and `LEADER_UNKNOWN` use the shared protocol
error envelope from [errors.md](../protocol/errors.md).

For non-leader controller startup, the same redirect semantics apply. A
non-leader controller fetches the current image from the leader controller for
crash recovery, deep-copies it, then publishes that image locally for read-only
controller state. It does not register itself as a broker, because
combined-role processes are rejected in this phase.

## API Surface

Controller quorum request and response schemas are defined in
[api.md](./api.md). This document describes controller behavior and state, while
the API document owns API names, request fields, response fields, redirect
behavior, and wire-schema expectations.

The APIs used by this phase are:

- `RegisterBrokerAndFetchMetadata`: used by brokers at startup. The leader
  controller appends `BrokerRegistrationRecord`, applies it to the metadata
  image, and returns the updated image.
- `FetchMetadataImage`: used by non-leader controllers at startup. The leader
  controller returns the current image without appending any broker registration
  record.

Topic-metadata mutation APIs, quorum consensus, and replication transport should
be specified later in separate controller or protocol design work.
