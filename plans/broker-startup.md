# Broker Startup Plan

## Purpose

Implement the first broker/controller startup slice for the Kafka clone.

This task creates the runtime path that starts a broker or controller from a
server properties file, establishes controller communication over the binary TCP
protocol, registers brokers with the leader controller, fetches metadata images,
and derives broker-local leader partition state.

## Architectural Decisions

- Startup is driven by `kafka-server-start.sh config.path=<path>`.
- Startup CLI validation is strict: `config.path` is required, exactly one
  `config.path` is allowed, and all unknown CLI arguments are rejected before
  reading any config file.
- The file at `config.path` is the server properties file. It may be named
  `server.properties`, `node.properties`, or any operator-chosen filename.
- Startup orchestration is single-threaded until the process reaches serving
  state. Runtime TCP request handling may use the protocol server model after
  startup has completed.
- Broker and controller startup share one common properties loader and common
  node validation rules.
- `process.roles` must contain exactly one role in this phase. Combined
  `broker,controller` nodes are rejected.
- Broker-role nodes must configure both `PLAINTEXT` and `CONTROLLER`
  listeners.
- Controller-role nodes must configure a `CONTROLLER` listener.
- `controller.quorum.voters` is required for both brokers and controllers.
- Controller leadership is static in this phase through `is.leader`; Raft is
  deferred.
- Non-leader controllers exclude their own endpoint when choosing a controller
  quorum peer during startup discovery.
- Broker-controller and controller-controller startup communication uses the
  binary TCP protocol defined in `docs/protocol/protocol.md`.
- The binary TCP protocol includes a shared request header with `APIKey` and
  `CorrelationID`, plus a shared response header with the matching
  `CorrelationID`.
- Controllers must support multiple in-flight startup requests on one
  connection so simultaneous broker startup does not serialize on response
  order.
- Quorum request and response wire types live under `internal/protocol`.
- Controller request handling may delegate to in-process services internally,
  but the end-to-end startup flow must use the real TCP client/server path.
- The concrete metadata log is abstracted behind a placeholder append/commit
  interface until the storage log component is implemented.
- A leader controller with no committed metadata records publishes an empty
  image for its `ClusterID` with `AppliedOffset = -1`.
- Metadata images returned to brokers and controllers are defensive deep copies.
- Broker registration is idempotent for restarts:
  - If the broker ID is new, append and apply `BrokerRegistrationRecord`.
  - If the broker ID already exists with matching metadata, return the current
    image without appending a duplicate record.
  - If the broker ID already exists with conflicting metadata, return
    `INVALID_BROKER_REGISTRATION`.
- `LeaderController` is runtime-only state stored in memory.
- Startup discovery retries retryable protocol errors and transport failures
  until a bounded startup timeout is reached.
- Startup communication uses a 10 second default timeout for TCP connect,
  request frame write, response frame read, and each full startup API call.
- Startup logs must include config-loaded, selected-controller,
  successful-registration, metadata-image-built, and fatal-failure events.

## Feature Scope

In scope:

- `kafka-server-start.sh` entry point.
- Server start command that reads `config.path`.
- Strict CLI argument validation.
- Shared server properties parsing.
- Shared `NodeConfig` validation.
- Controller-specific config validation for `is.leader` and
  `metadata.log.dir`.
- Broker startup discovery.
- Non-leader controller metadata fetch discovery.
- TCP listener startup for controller `CONTROLLER` endpoints.
- TCP client path for broker-controller and controller-controller startup APIs.
- Binary frame encoding/decoding for:
  - `RegisterBrokerAndFetchMetadata`
  - `FetchMetadataImage`
  - protocol error envelope
  - controller redirect endpoint
  - metadata image response
- Leader controller request handlers for:
  - broker registration and metadata fetch
  - read-only metadata image fetch
- Non-leader controller redirect behavior:
  - `NOT_LEADER` with leader endpoint when known
  - `LEADER_UNKNOWN` when leader is unknown
- Broker restart idempotency for matching existing broker metadata.
- Empty metadata image publication with `AppliedOffset = -1`.
- Broker-local metadata image storage after startup.
- Broker-local leader partition state derivation from the fetched metadata
  image.
- Defensive copy behavior for metadata images and leader partition reads.
- Reusable test fixture properties files.
- Unit tests for component behavior and Docker Compose integration tests for
  the real startup path.
- Docker Compose is the primary integration test mechanism for this task.
- At least one Docker Compose startup integration test with controller and
  broker services.

Out of scope:

- Raft leader election.
- Metadata-log storage implementation using real segmented logs.
- Controller-to-controller metadata-log replication.
- Produce and fetch APIs.
- Topic mutation APIs.
- Broker heartbeat, fencing, shutdown, or metadata update records.
- Dynamic config reload.
- TLS, SASL, authorization, compression, and Kafka wire compatibility.
- Consumer group startup behavior.

## Implementation Steps

1. Create startup command structure.
   - Add the `kafka-server-start.sh` shell entry point under `cmd` or scripts
     according to the repo convention chosen during implementation.
   - Add a Go server start command that accepts `config.path=<path>`.
   - Reject missing `config.path`, duplicate `config.path`, and unknown CLI
     arguments before reading config.

2. Implement shared config loading.
   - Parse Java-properties-style `key=value` files.
   - Reject malformed property lines.
   - Parse and validate `cluster.id`, `process.roles`, `node.id`, `listeners`,
     `advertised.listeners`, `listener.security.protocol.map`,
     `inter.broker.listener.name`, `controller.listener.names`,
     `controller.quorum.voters`, and `log.dirs`.
   - Reject combined roles.
   - Enforce required listeners by role.
   - Default security protocol to `PLAINTEXT` where allowed.

3. Implement controller-specific config loading.
   - Parse `is.leader`.
   - Parse `metadata.log.dir`.
   - Require `controller.quorum.voters`.
   - Reuse shared node validation.

4. Implement metadata image types and store.
   - Define minimal `metadata.TopicConfig`.
   - Define metadata image, broker metadata, topic metadata, partition metadata,
     and partition key types.
   - Implement empty image creation with `AppliedOffset = -1`.
   - Implement deep-copy behavior.
   - Implement image lookup by cluster ID.

5. Implement placeholder metadata-log abstraction.
   - Define an append/commit interface for broker registration records.
   - Provide an in-memory implementation for this phase.
   - Ensure duplicate broker registration records are not appended for broker
     restarts with matching metadata.

6. Implement protocol types and binary encoding.
   - Define protocol error codes and envelopes.
   - Define controller redirect endpoint encoding.
   - Define shared request and response headers.
   - Define request type routing for controller listener frames through
     `APIKey`.
   - Allocate positive `CorrelationID` values per connection and track
     in-flight requests by ID.
   - Copy the request `CorrelationID` into every response.
   - Reject duplicate in-flight `CorrelationID` values on one connection.
   - Encode/decode `RegisterBrokerAndFetchMetadata`.
   - Encode/decode `FetchMetadataImage`.
   - Encode/decode metadata image responses.
   - Apply frame size limits and malformed frame handling.

7. Implement TCP transport.
   - Implement length-prefixed frame reader/writer.
   - Implement controller listener accept loop.
   - Dispatch controller startup API requests to handlers by `APIKey`.
   - Support multiple in-flight startup requests on a single connection.
   - Match responses to requests by `CorrelationID`, not response order.
   - Implement client calls with 10 second connect, frame write, response read,
     and startup API call deadlines.
   - Close failed or malformed connections.

8. Implement leader controller handlers.
   - Validate request cluster ID and node IDs.
   - Create or load the image for `ClusterID`.
   - For new broker IDs, append and apply `BrokerRegistrationRecord`.
   - For matching existing broker IDs, return the current image without append.
   - For conflicting existing broker IDs, return
     `INVALID_BROKER_REGISTRATION`.
   - Return defensive image copies.

9. Implement non-leader controller behavior.
   - Return `NOT_LEADER` with known leader endpoint.
   - Return `LEADER_UNKNOWN` when leader is unknown.
   - On startup, fetch metadata from the leader over TCP.
   - Exclude the controller's own endpoint during discovery.
   - Publish the fetched image locally after validating `ClusterID`.

10. Implement broker startup discovery.
    - Choose a random controller endpoint from `ControllerQuorum`.
    - Send `RegisterBrokerAndFetchMetadata` over TCP.
    - Follow `NOT_LEADER` redirects with leader endpoint.
    - Treat `NOT_LEADER` without leader endpoint like `LEADER_UNKNOWN`.
    - Retry `LEADER_UNKNOWN`, `INTERNAL_ERROR`, and transport failures until
      the startup timeout is reached.
    - Store `LeaderController` in memory after successful discovery.
    - Validate returned metadata image `ClusterID`.

11. Implement broker runtime startup state.
    - Store the fetched metadata image in memory.
    - Build leader partition state from partitions whose `LeaderBrokerID`
      matches the local `NodeID`.
    - Expose defensive leader partition read APIs.

12. Wire server start by role.
    - Controller startup order: parse CLI args, parse config, validate config,
      initialize metadata store and publish an empty image when needed, fetch
      metadata if non-leader, then begin serving controller requests.
    - Broker startup order: parse CLI args, parse config, validate config,
      connect to a controller endpoint, register broker, fetch metadata image,
      build leader partition state, then complete startup.
    - Keep startup sequencing single-threaded until serving begins.

13. Add startup logging.
    - Log config loaded successfully with role, node ID, and cluster ID.
    - Log selected controller endpoint with node ID, host, and port.
    - Log successful broker registration with broker ID and cluster ID.
    - Log metadata image built successfully with cluster ID and applied offset.
    - Log fatal startup failures before returning or exiting.

14. Enforce immediate abort behavior.
    - Abort on CLI validation failure before reading config.
    - Abort on missing, unreadable, or malformed config file.
    - Abort on invalid `NodeConfig`, unsupported role, combined roles, missing
      required listeners, invalid quorum, invalid cluster ID, multiple log
      directories, or invalid controller fields.
    - Abort on controller metadata store/image initialization failure.
    - Abort on controller listener bind failure.
    - Abort on non-leader controller metadata fetch failure after timeout or
      terminal protocol error.
    - Abort on broker discovery, registration, or metadata fetch failure after
      timeout or terminal protocol error.
    - Abort on fetched metadata image cluster mismatch.
    - Abort on conflicting duplicate broker registration.
    - Abort on leader partition state build failure.

## Testing Steps

1. Add reusable config fixtures.
   - Leader controller properties.
   - Non-leader controller properties.
   - Broker properties.
   - Invalid combined-role properties.
   - Invalid listener and invalid quorum properties.

2. Test public startup config behavior.
   - `config.path` is required.
   - Duplicate `config.path` is rejected.
   - Unknown CLI arguments are rejected.
   - A valid single `config.path` proceeds to config loading.
   - Valid broker config loads.
   - Valid controller config loads.
   - Combined roles are rejected.
   - Broker missing `CONTROLLER` listener is rejected.
   - Controller missing `CONTROLLER` listener is rejected.
   - Invalid `is.leader` and missing `metadata.log.dir` are rejected.

3. Test metadata image behavior.
   - Empty image starts with `AppliedOffset = -1`.
   - Defensive image copies protect stored maps and slices.
   - Broker metadata is added from registration records.
   - Duplicate broker registration records are rejected during replay/apply.

4. Test leader controller handlers.
   - New broker registration appends, applies, and returns image.
   - Broker restart with matching metadata returns image without append.
   - Broker restart with conflicting metadata returns
     `INVALID_BROKER_REGISTRATION`.
   - `FetchMetadataImage` returns image without appending records.
   - Cluster mismatch returns the expected error code.

5. Test redirect and retry behavior.
   - Non-leader returns `NOT_LEADER` with leader endpoint when known.
   - Non-leader returns `LEADER_UNKNOWN` when leader is unknown.
   - Broker follows `NOT_LEADER` redirect.
   - Broker treats `NOT_LEADER` without endpoint like `LEADER_UNKNOWN`.
   - Broker retries `LEADER_UNKNOWN`, `INTERNAL_ERROR`, and transport failures.
   - Discovery fails after startup timeout.
   - Startup communication uses the 10 second default timeout unless a test
     override is injected.
   - Non-leader controller excludes its own endpoint during discovery.

6. Test protocol encoding and transport.
   - Frame reader/writer handles complete frames.
   - Malformed or oversized frames are rejected.
   - Unknown `APIKey` values are rejected.
   - Duplicate in-flight `CorrelationID` values on one connection are rejected.
   - `RegisterBrokerAndFetchMetadata` round-trips over TCP.
   - `FetchMetadataImage` round-trips over TCP.
   - Protocol error envelopes round-trip over TCP.
   - Multiple broker registration requests can be in flight on one controller
     connection.
   - Responses are matched by `CorrelationID` when they arrive out of order.

7. Test broker leader partition state.
   - Includes partitions led by local broker.
   - Excludes partitions led by other brokers.
   - Rebuild replaces stale leadership state.
   - Read APIs return defensive copies.

8. Add Docker-first integration tests for the real startup flow.
   - Use Docker Compose as the primary integration test mechanism for startup.
   - Compose should launch real processes from fixture config files instead of
     replacing the integration flow with injected in-process clients.
   - Start a leader controller from fixture config.
   - Start a broker from fixture config and verify registration through TCP.
   - Restart the broker with the same config and verify idempotent registration.
   - Add at least one Docker Compose startup test with `controller` and
     `broker` services.
   - Docker Compose flow: build binary image, start controller containers, wait
     for controller ports to become healthy, start broker containers, assert
     brokers register successfully, restart broker containers, assert duplicate
     registration is not created, then stop containers.
   - The Docker Compose startup test should start multiple broker containers at
     the same time to exercise concurrent registration and correlation ID
     response matching.
   - Document and use these commands for the Docker flow:
     `docker compose up --build -d`, `docker compose logs`, and
     `docker compose down -v`.

9. Test startup logging and abort behavior.
   - Successful startup emits config-loaded, selected-controller,
     successful-registration, and metadata-image-built logs.
   - Fatal startup failures emit a fatal log entry before exit.
   - Invalid config aborts before network work.
   - Controller metadata store init failure aborts before serving.
   - Controller bind failure aborts startup.
   - Broker registration timeout aborts startup.
   - Conflicting existing broker registration aborts startup.
   - Fetched metadata cluster mismatch aborts startup.
   - Controller does not serve before config validation and metadata
     initialization.
   - Non-leader controller fetches metadata before serving.
   - Broker builds leader partition state only after metadata fetch succeeds.

10. Run verification.
   - `go test ./...`
   - Docker Compose startup integration test command added for this phase.
