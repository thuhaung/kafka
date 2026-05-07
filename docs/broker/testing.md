# Broker Testing

## Purpose

This document defines validation rules, failure modes, test expectations, and
known unresolved design issues for the broker startup and runtime metadata
layer.

## Validation Rules

The broker loader and startup path must enforce:

- `node.id` parses as an integer.
- `cluster.id` is present and parses as a UUID.
- `process.roles` contains exactly one supported role.
- `listeners` defines only supported listener types.
- `listeners` contains at most one `PLAINTEXT` listener.
- `listeners` contains at most one `CONTROLLER` listener.
- `listener.security.protocol.map` resolves configured listener types to
  `PLAINTEXT`.
- `inter.broker.listener.name` is `PLAINTEXT`.
- `controller.listener.names` is `CONTROLLER`.
- `controller.quorum.voters` parses into one or more controller endpoints for
  broker and controller nodes.
- `log.dirs` resolves to exactly one local directory in this phase.
- Broker-role nodes configure both `PLAINTEXT` and `CONTROLLER` listeners.
- Controller-role nodes configure a `CONTROLLER` listener.

## Failure Modes

The broker implementation should explicitly handle:

- missing `config.path`
- duplicate `config.path`
- unknown CLI argument
- unreadable properties file at `config.path`
- malformed property lines
- invalid `node.id`
- unsupported role names
- multiple configured roles in `process.roles`
- unsupported listener names
- duplicate listener types
- missing required listener for the configured role
- invalid or missing cluster ID
- invalid `controller.quorum.voters` formatting
- empty controller quorum for a broker-role node
- failure to contact the chosen controller during initial metadata fetch
- failure to contact the returned leader address after a `NOT_LEADER` response
- a controller returning `LEADER_UNKNOWN` because it has not yet learned the
  leader
- a controller returning `NOT_LEADER` without a leader endpoint
- startup timeout reached while retrying controller discovery
- controller rejecting a metadata fetch because of an unknown cluster ID
- controller rejecting broker registration because the requested broker metadata
  conflicts with the existing registration for the same node ID
- failure to build leader partition state from the returned metadata image
- invalid host or port formatting
- multiple log directories configured in this first phase

Expected behavior:

- Startup fails fast on invalid configuration.
- Fatal startup failures are logged before the process exits or returns an
  error.
- The broker does not continue with partial or defaulted critical metadata,
  except for the default security protocol of `PLAINTEXT`.
- Runtime components read already-validated metadata from memory instead of
  reparsing raw properties.
- Startup discovery fails only after the broker exhausts the configured
  controller quorum, reaches the configured startup timeout, or a terminal error
  is returned.
- Leader partition state is rebuilt only from a successfully published metadata
  image.

## Testing Strategy

Startup-path public API tests should cover:

- rejection of missing `config.path`
- rejection of duplicate `config.path`
- rejection of unknown CLI arguments
- verification that a valid single `config.path` proceeds to config loading
- loading a valid properties file from `config.path` into `NodeConfig`
- starting a broker role from `kafka-server-start.sh config.path=<path>`
- rejection of `process.roles=broker,controller`
- rejection of missing required listener by role
- verification that broker-role startup attempts registration and metadata fetch
  after config load
- verification that startup discovery chooses a random controller quorum address
- verification that a `NOT_LEADER` response includes the leader node ID and
  leader address
- verification that startup discovery stores the resolved leader in memory as
  `LeaderController`
- verification that a `NOT_LEADER` response causes a retry against
  `LeaderController`
- verification that a `LEADER_UNKNOWN` response causes a retry against another
  quorum endpoint
- verification that `NOT_LEADER` without a leader endpoint is treated like
  `LEADER_UNKNOWN`
- verification that `INTERNAL_ERROR` is retried during startup discovery until
  timeout
- verification that startup communication uses the `10s` default timeout unless
  a test override is injected
- verification that startup discovery fails only after exhausting the configured
  controller quorum or reaching the startup timeout
- verification that fetched metadata image cluster ID mismatch fails startup
- verification that broker restart registration succeeds when the same broker
  ID is already registered with matching metadata
- verification that broker restart registration fails when the same broker ID is
  already registered with conflicting metadata
- building leader partition state from a fetched metadata image
- including only partitions whose `LeaderBrokerID` matches local `NodeID`
- excluding partitions led by other brokers
- rebuilding leader partition state after a metadata image update
- removing partitions when this broker is no longer the leader
- returning defensive copies from leader partition lookup APIs
- verification that successful startup emits config-loaded,
  selected-controller, successful-registration, and metadata-image-built logs
- verification that fatal startup failures emit a fatal log entry
- verification that invalid config aborts before network work
- verification that broker registration timeout aborts startup
- verification that broker builds leader partition state only after metadata
  fetch succeeds

## Test Contracts

Startup discovery tests should use reusable test properties files. Unit tests
may use injected controller clients so retry and redirect behavior can be
verified quickly without depending on incidental map iteration, wall-clock
timing, or network availability.

Docker Compose is the primary integration test mechanism for this startup
phase. The real broker-controller startup flow over the binary TCP protocol
defined in [protocol.md](../protocol/protocol.md) should be verified by
launching real controller and broker processes from fixture config files. The
compose file should include `controller` and `broker` services.

The Docker Compose test flow is:

1. Build the binary image.
2. Start controller containers.
3. Wait for controller ports to become healthy.
4. Start multiple broker containers at the same time.
5. Assert brokers register successfully.
6. Restart broker containers.
7. Assert duplicate registration is not created.
8. Stop containers.

The simultaneous broker startup step must exercise multiple in-flight broker
registration requests and verify responses are matched by `CorrelationID`, not
by response order.

The documented commands for the Docker flow are:

```sh
docker compose up --build -d
docker compose logs
docker compose down -v
```

In-process or injected-client tests may supplement this coverage, but they are
not a substitute for the Docker Compose startup integration test.

Random controller choice means the startup client should not always contact the
first endpoint in `controller.quorum.voters`. The implementation should allow
tests to inject a deterministic chooser or ordered endpoint plan so retry order
can be asserted.

Leader partition state tests should use metadata images with explicit broker
IDs, topic names, and partition IDs. Tests must assert that callers cannot
mutate stored state through returned slices, maps, or metadata values.

## Open Items

The following items are intentionally left for later definition:

- replication APIs
- exact consumer group coordinator wire encoding, as described in
  [api.md](../group_coordinator/api.md)
- dynamic config reload behavior
