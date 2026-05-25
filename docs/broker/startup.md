# Broker Startup and Runtime Flow

## Startup Flow

Broker startup should follow this sequence:

1. Start through the `kafka-server-start.sh` CLI program
2. Parse CLI arguments
3. Require exactly one `config.path` argument
4. Reject unknown CLI arguments
5. Read the server properties file from `config.path`
6. Parse key-value properties
7. Convert parsed values into a `NodeConfig`
8. Validate required fields and listener constraints
9. Keep the resulting `NodeConfig` in memory for the lifetime of the process
10. If the node role is `broker`, invoke the startup discovery API
11. The startup discovery API picks a random controller from `ControllerQuorum`
12. The broker sends `RegisterBrokerAndFetchMetadata` to that controller using
   `ClusterID`, `NodeID`, and broker contact metadata
13. If the controller returns `NOT_LEADER` with the current leader node ID and
    leader address, the broker stores that leader in memory as
    `LeaderController`
14. The broker retries the same request against `LeaderController`
15. If the controller returns `NOT_LEADER` without a leader endpoint,
    `LEADER_UNKNOWN`, or cannot be reached, the broker tries another controller
    from `ControllerQuorum`
16. On success, the broker keeps `LeaderController` in memory for later
    controller communication
17. Cache or hand off the returned metadata image to the runtime components
    that need it
18. Build the broker-local leader partition state from the returned metadata
    image
19. Use the in-memory `NodeConfig` whenever the process needs its local node
    metadata

The broker-role startup order is:

1. Parse CLI args.
2. Parse config.
3. Validate config.
4. Connect to a controller endpoint.
5. Register broker.
6. Fetch metadata image.
7. Build leader partition state.
8. Complete startup.

The process should not repeatedly reread the file during normal operation. If
the broker crashes and restarts, the operator starts it again with
`kafka-server-start.sh config.path=<path>`, and the broker loads the same
configuration path again during the new process startup.

Startup orchestration must be single-threaded until broker startup completes.
This means each startup step above runs sequentially in one control flow.
Runtime TCP request handling may use the protocol server model only after the
process reaches its serving state.

## Runtime Access Model

After startup, the broker should treat the parsed `NodeConfig` as the local
source of truth for static node metadata.

During the node's lifetime:

- client-facing networking uses the configured `PLAINTEXT` listener metadata
- broker-to-broker communication uses the configured `PLAINTEXT` listener
  metadata
- broker-to-controller communication uses `LeaderController` when the node role
  is `broker` and discovery has completed
- local storage paths use `LogDir`
- role checks use the `Role` field
- leader-only partition work uses the broker-local leader partition state

## Leader Partition State

Each broker must maintain an in-memory view of the partitions it currently
leads.

This state is derived from the cluster metadata image defined in
[cluster_metadata.md](../metadata/cluster_metadata/cluster_metadata.md). The broker should not persist a separate
leader-partition file or treat this local view as an authority independent of
the metadata image.

The local state contains every partition whose `LeaderBrokerID` equals the
broker's local `NodeID`.

Recommended shape:

```go
type LeaderPartitionState struct {
	BrokerID   int
	Partitions map[metadata.PartitionKey]metadata.PartitionMetadata
}
```

Derivation rules:

1. Start from the current published metadata image.
2. Iterate over `Image.Partitions`.
3. Select partitions where `PartitionMetadata.LeaderBrokerID == NodeID`.
4. Store copies of those partition metadata entries in `LeaderPartitionState`.
5. Publish the derived state atomically after the metadata image is published.

Runtime rules:

- Reads of leader partition state should be defensive: callers must not mutate
  the stored map or partition metadata.
- If the metadata image changes, the broker must rebuild this state from the new
  image instead of patching it from stale local assumptions.
- If a partition leadership change removes this broker as leader, the rebuilt
  state must remove that partition before leader-only work continues.
- If a partition leadership change makes this broker the leader, the rebuilt
  state must add that partition before leader-only work starts.
- Produce, fetch, and replication behavior can later use this state to decide
  whether the local broker should handle leader responsibilities or redirect.

This state is a local cache of metadata-image facts. The controller and metadata
log remain the source of truth for partition leadership.

## Startup Metadata Fetch

After loading `NodeConfig`, a node with role `broker` must register with the
controller quorum and fetch cluster metadata.

The first-phase behavior is:

- the broker reads `controller.quorum.voters` from the properties file supplied
  by `config.path`
- the broker picks a random controller endpoint from that quorum configuration
- the broker sends `RegisterBrokerAndFetchMetadata` using `ClusterID`,
  `NodeID`, and its configured listener host and port
- if the contacted controller is not the leader, it returns `NOT_LEADER`
  together with the actual leader node ID and leader address
- the broker stores that returned endpoint in memory as `LeaderController`
- the broker retries the same registration and fetch request against
  `LeaderController`
- if the contacted controller returns `NOT_LEADER` without a leader endpoint,
  the broker treats it like `LEADER_UNKNOWN`
- if the contacted controller is not the leader and does not know who the
  leader is yet, it returns `LEADER_UNKNOWN`
- if the contacted controller is unreachable, returns `LEADER_UNKNOWN`, or
  returns `NOT_LEADER` without a leader endpoint, the broker tries another
  controller from the remaining quorum endpoints
- if a returned `LeaderController` is unreachable, the broker continues trying
  known quorum endpoints until the startup timeout is reached
- if every configured controller returns `LEADER_UNKNOWN` or `NOT_LEADER`
  without a leader endpoint before any leader endpoint is learned, startup
  fails
- if this broker ID is not already registered, the leader controller appends and
  commits `BrokerRegistrationRecord`, applies it to the metadata image, and
  responds with the current metadata image
- if this broker ID is already registered with matching broker metadata, the
  leader controller treats the request as a broker restart and responds with the
  current metadata image without appending a duplicate registration record
- if this broker ID is already registered with conflicting broker metadata, the
  leader controller rejects startup with `INVALID_BROKER_REGISTRATION`
- on success, the broker keeps `LeaderController` for later
  controller-directed requests
- that response includes information about all brokers in the cluster and
  partition leaders
- the broker derives its local leader partition state from the returned metadata
  image
- the detailed response shape is defined in [api.md](../quorum/api.md)

The important requirement in this phase is that the broker can begin from any
controller quorum address, follow a `NOT_LEADER` redirect once it learns the
leader address, continue trying other quorum members when leader information is
still unknown, keep retrying until the configured startup timeout is reached,
and receive a metadata image that includes its own registration.

The default startup communication timeout is `10s`. This timeout applies to TCP
connect, request frame write, response frame read, and each full startup API
call. The discovery loop must not retry forever.

## Startup Logging

Minimum startup logs should include:

- config loaded successfully, including role, node ID, and cluster ID
- selected controller endpoint, including controller node ID, host, and port
- successful broker registration, including broker ID and cluster ID
- metadata image built successfully, including cluster ID and applied offset
- fatal startup failures before the process exits or returns an error

## Immediate Abort Conditions

Startup must abort immediately and must not continue when any of these
conditions occur:

- missing `config.path`
- duplicate `config.path`
- unknown CLI argument
- unreadable file at `config.path`
- malformed property lines
- invalid `node.id`
- unsupported role name
- multiple configured roles in `process.roles`
- unsupported listener name
- duplicate listener type
- missing required listener
- invalid or missing cluster ID
- invalid `controller.quorum.voters` formatting
- empty controller quorum for a broker-role or controller-role node
- missing or invalid `log.dir`
- failure to contact a controller before the startup timeout
- terminal protocol error from the controller
- fetched metadata image cluster ID mismatch
- controller rejects broker registration because the requested broker metadata
  conflicts with the existing registration for the same node ID
- failure to build leader partition state from the returned metadata image

## Failure Modes

The broker configuration layer should explicitly handle:

- missing `config.path`
- duplicate `config.path`
- unknown CLI argument
- unreadable file at `config.path`
- malformed property lines
- invalid `node.id`
- unsupported role names
- multiple configured roles in `process.roles`
- unsupported listener names
- duplicate listener types
- missing required listener
- invalid or missing cluster ID
- invalid `controller.quorum.voters` formatting
- empty controller quorum for a broker-role or controller-role node
- failure to contact the chosen controller during initial metadata fetch
- failure to contact the returned leader address after a `NOT_LEADER` response
- a controller returning `LEADER_UNKNOWN` because it has not yet learned the
  leader
- a controller returning `NOT_LEADER` without a leader endpoint
- controller rejecting a metadata fetch because of an unknown cluster ID
- controller rejecting a broker restart because the requested broker metadata
  conflicts with the existing registration for the same node ID
- failure to build leader partition state from the returned metadata image
- invalid host or port formatting
- missing or invalid `log.dir`

Behaviors:

- startup should fail fast on invalid configuration
- fatal startup failures should be logged before the process exits or returns an
  error
- the broker should not continue with partial or defaulted critical metadata
  except for the default security protocol of `PLAINTEXT`
- runtime components should read already-validated metadata from memory rather
  than reparsing raw properties
- startup discovery should fail only after the broker exhausts the configured
  controller quorum, reaches the configured startup timeout, or a terminal error
  is returned
- leader partition state should be rebuilt only from a successfully published
  metadata image
