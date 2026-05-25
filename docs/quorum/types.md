# Controller Quorum Types

## Purpose

This document defines the controller metadata model, server properties mapping,
runtime access model, metadata image ownership, and recommended Go types for the
controller quorum implementation.

## Metadata Model

The controller uses the shared node metadata model from
[broker.md](../broker/broker.md) for common fields such as `ClusterID`,
`NodeID`, `Role`, `LogDir`, listeners, security protocol, controller quorum
endpoints, and `LeaderController`.

Controller-specific additions are:

| Field | Type | Required | Description |
| --- | --- | --- | --- |
| MetadataLogDir | string | Yes | Local directory containing the controller metadata log |
| IsLeaderController | bool | Yes | Persisted temporary leader flag used until Raft-based leader election exists |

Controller nodes must still satisfy the shared listener constraints from
[broker.md](../broker/broker.md). In addition, a controller process must run
with exactly the `controller` role and should keep the shared single
listener-plus-protocol-map configuration used in this phase.

## Server Start CLI and Properties Format

Each controller is started through `kafka-server-start.sh`. The CLI accepts a
required `config.path` parameter that points to a properties file on local disk:

```sh
kafka-server-start.sh config.path=/path/to/controller.properties
```

The file may be named `node.properties`, `server.properties`, or any other
operator-chosen name. The startup contract is:

1. The process receives `config.path` before startup work begins.
2. It reads the properties file at `config.path`.
3. It parses the file once during startup.
4. It stores the parsed result in memory for later use.

Example:

```properties
cluster.id=4f3e7f6e-7c46-4f9f-b0db-2dcf6d3c6c14
process.roles=controller
node.id=1
listeners=CONTROLLER://localhost:9093
listener.security.protocol.map=CONTROLLER:PLAINTEXT
controller.quorum.voters=1@localhost:9093,2@localhost:9094,3@localhost:9095
is.leader=true
metadata.log.dir=kraft-cluster/controller1/metadata-log
log.dir=kraft-cluster/controller1
```

## Field Mapping

The initial implementation should reuse the common property mapping from
[broker.md](../broker/broker.md) for `cluster.id`, `process.roles`, `node.id`,
`listeners`, `listener.security.protocol.map`, `controller.quorum.voters`, and
`log.dir`.

Controller-specific properties are:

| Property | Maps To | Notes |
| --- | --- | --- |
| `is.leader` | IsLeaderController | Persisted temporary leader flag |
| `metadata.log.dir` | MetadataLogDir | Local directory containing the metadata log |

### Cluster ID

`ClusterID` follows the shared node semantics in
[broker.md](../broker/broker.md). The controller additionally uses it to choose
the metadata image served to brokers during cluster metadata fetches.

### Leader Flag

`IsLeaderController` is persisted under the `is.leader` key.

This flag exists only because Raft leader election is not implemented yet. Until
Raft exists, controller leadership is determined from static configuration
rather than quorum consensus.

### Metadata Log Directory

`MetadataLogDir` is persisted under the `metadata.log.dir` key.

This path points to the location of the controller metadata log, which stores
cluster metadata such as topic state and other control-plane records defined
later.

In this phase, that metadata log should be treated as one logical cluster
metadata partition replicated across the controller quorum. Each controller
keeps its own local on-disk replica under `MetadataLogDir`; controllers do not
share one filesystem path.

### Listener and Quorum Voters

The configured `listeners` entry is also the cluster-reachable advertised
address other clients, brokers, and controllers use to connect to this node in
this phase.

The configured listener alias must resolve to `PLAINTEXT` through
`listener.security.protocol.map`.

`controller.quorum.voters` is the controller bootstrap map keyed by controller
node ID. Its host and port values should match each controller node's configured
listener so startup discovery and redirect responses use the same reachable
controller address.

## Proposed Go Types

The controller package should reuse or mirror the shared node configuration
types from [broker.md](../broker/broker.md). It should add only the fields that
are controller-specific.

The exact package path may evolve, but `internal/controller` should expose a
type close to:

```go
package controller

type NodeConfig struct {
	// Shared node fields match the structure documented in broker.md.
	ClusterID          string
	NodeID             int
	Role               Role
	LogDir             string
	ListenerConfig     ListenerConfig
	ControllerQuorum   []ControllerEndpoint
	LeaderController   *ControllerEndpoint
	SecurityProtocol   string

	// Controller-specific fields.
	MetadataLogDir     string
	IsLeaderController bool
}
```

Implementation notes:

- `MetadataLogDir` must be present and must point to the metadata-log location.
- `IsLeaderController` is loaded directly from disk because Raft is not used
  yet.
- `LeaderController` is not loaded from disk and is populated only after
  successful startup discovery for non-leader controllers.
- `ControllerQuorum` is required for controller nodes. Non-leader controllers
  use it for startup discovery, excluding their own endpoint. Leader
  controllers keep it available for the later Raft-based implementation.
- All common parsing and validation rules are inherited from
  [broker.md](../broker/broker.md).

## Runtime Access Model

After startup, the controller should treat the parsed `NodeConfig` as the local
source of truth for static controller metadata.

During the node's lifetime:

- broker-to-controller communication uses the shared configured listener
  metadata
- controller-local metadata-log access uses `MetadataLogDir`
- any controller-local auxiliary storage uses `LogDir`
- leader checks use the `IsLeaderController` field
- non-leader controllers use `LeaderController` after startup discovery
  completes
- cluster identity checks use `ClusterID`
- metadata reads use the current published metadata image for `ClusterID`

## Cluster Metadata Image

The controller maintains an in-memory metadata image using the structure defined
in [cluster_metadata.md](../metadata/cluster_metadata/cluster_metadata.md). The
controller should not define a separate topic, partition, or broker metadata
structure in this document.

At runtime, the controller keeps a map of images keyed by cluster ID.

The first implementation may only ever contain one cluster ID, but keying the
store by `ClusterID` keeps the controller API explicit and matches the startup
fetch flow.

If replay finds no metadata records for the configured cluster, the controller
publishes an empty image for `ClusterID` with `AppliedOffset = -1`. A leader
controller may serve broker registration and metadata fetch requests from this
empty image, applying the first accepted registration record to create the first
broker entry.

The controller constructs or advances this image in three cases:

- during replay, by reading committed records from an abstract metadata-log
  reader in offset order and applying them to an empty image
- during replication, by applying committed records received from the leader
  controller to the follower controller's image
- during a new state change on the leader controller, by appending the accepted
  metadata record through an abstract metadata-log append/commit interface and
  then applying the committed record to the leader's image

Image construction follows the replay and immutability rules in
[cluster_metadata.md](../metadata/cluster_metadata/cluster_metadata.md):
records are validated against the image built so far, updates produce a new
published image, and callers read a stable snapshot instead of mutating the
image directly.

The image contains the metadata brokers need at startup, including:

- all known brokers, keyed by broker ID
- topics, keyed by topic name
- partitions, keyed by topic name and partition ID
- each partition's current leader broker ID, replica set, ISR, and leader epoch

The leader controller is the only controller that should accept new metadata
state changes in this phase. Non-leader controllers maintain local images from
metadata fetched from the leader, but they should not publish uncommitted local
decisions as cluster metadata.

More concretely, every controller stores the same committed metadata-partition
history in its own local metadata-log directory, modulo temporary follower lag.
That shared committed record order is what allows each controller to rebuild
the same metadata image independently from local storage.
