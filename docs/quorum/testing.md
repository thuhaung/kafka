# Controller Quorum Testing

## Purpose

This document defines validation rules, failure modes, test expectations, and
known unresolved design issues for the controller quorum component.

## Validation Rules

The startup loader should reuse the shared node validation rules from
[broker.md](../broker/broker.md). Controller-specific rules are:

- `process.roles` must be exactly `controller`.
- `is.leader` must parse as a boolean.
- `metadata.log.dir` must be present and non-empty.
- Both configured `PLAINTEXT` and `CONTROLLER` listeners must be present.
- The loaded or fetched metadata image must match `ClusterID`.
- A controller must parse `controller.quorum.voters`, even when it is currently
  configured as the leader controller.
- Broker startup registration must not publish broker metadata for a different
  cluster ID.
- Broker startup registration must return the current metadata image without
  appending a duplicate record if the current image already contains matching
  metadata for the requested broker ID.
- Broker startup registration must fail if the current image already contains
  the requested broker ID with conflicting broker metadata.

## Failure Modes

The controller configuration layer should explicitly handle:

- missing `config.path`
- duplicate `config.path`
- unknown CLI argument
- unreadable properties file at `config.path`
- malformed property lines
- non-controller role configured for the controller process
- missing required `CONTROLLER` listener
- invalid `is.leader` formatting
- missing or invalid `metadata.log.dir`
- a contacted non-leader controller returning `LEADER_UNKNOWN`
- a contacted non-leader controller returning `NOT_LEADER` without a leader
  endpoint
- startup timeout reached while retrying controller discovery
- metadata image replay failure
- metadata store or image initialization failure
- controller listener bind failure
- broker registration for the wrong cluster ID
- broker registration with invalid broker metadata
- broker restart registration for a broker ID that already exists with
  conflicting metadata in the current image
- failure to append or commit the broker registration record

Common malformed node, listener, quorum, and log-directory failures are inherited
from [broker.md](../broker/broker.md).

Expected behavior:

- Startup should fail fast on invalid configuration.
- Fatal startup failures should be logged before the process exits or returns an
  error.
- The controller should not continue with partial or defaulted critical metadata
  except for the default security protocol of `PLAINTEXT`.
- Runtime components should read already-validated metadata from memory rather
  than reparsing raw properties.
- Non-leader startup discovery should fail only after the controller exhausts
  the configured controller quorum, reaches the configured startup timeout, or a
  terminal error is returned.
- The leader controller should return a metadata image only after any accepted
  broker registration record has been committed and applied.

## Testing Strategy

The controller startup implementation should include public API tests covering:

- rejection of missing `config.path`
- rejection of duplicate `config.path`
- rejection of unknown CLI arguments
- verification that a valid single `config.path` proceeds to config loading
- loading a valid controller properties file from `config.path` into
  `NodeConfig`
- starting a controller role from `kafka-server-start.sh config.path=<path>`
- rejection of `process.roles=broker`
- rejection of `process.roles=broker,controller`
- rejection of invalid `is.leader` format
- rejection of missing `metadata.log.dir`
- rejection of missing required `CONTROLLER` listener
- verification that non-leader controller startup attempts `FetchMetadataImage`
  after config load
- verification that non-leader startup discovery chooses a random controller
  quorum address
- verification that non-leader startup discovery excludes its own controller
  endpoint
- verification that a `NOT_LEADER` response includes the leader node ID and
  leader address
- verification that non-leader startup discovery stores the resolved leader in
  memory as `LeaderController`
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
- verification that non-leader startup discovery fails only after exhausting the
  configured controller quorum or reaching the startup timeout
- defaulting security protocol to `PLAINTEXT` when appropriate
- publishing an empty image keyed by `ClusterID` with `AppliedOffset = -1` when
  no committed metadata records exist
- applying a committed state change from the abstract metadata-log interface to
  the leader controller image
- returning the current image for a cluster ID
- rejecting image fetches for an unknown cluster ID
- appending a broker registration record before returning metadata to a broker
- applying broker registration to the image's broker map
- returning brokers and partition leaders in the fetched image
- rejecting broker registration for a mismatched cluster ID
- returning the current metadata image without appending a duplicate record when
  broker startup registration repeats with identical metadata
- rejecting broker registration when the broker ID already exists in the image
  with conflicting metadata
- preserving the previous image when broker registration or metadata replay
  fails
- verification that successful startup emits config-loaded,
  selected-controller where applicable, and metadata-image-built logs
- verification that fatal startup failures emit a fatal log entry
- verification that controller metadata store init failure aborts before serving
- verification that controller bind failure aborts startup
- verification that controller does not serve before config validation and
  metadata initialization
- verification that non-leader controller fetches metadata before serving

API-specific tests should cover:

- successful broker registration and metadata image fetch
- successful read-only metadata image fetch for a non-leader controller
- `NOT_LEADER` responses with leader controller endpoints
- `LEADER_UNKNOWN` responses without leader controller endpoints
- rejection of cluster ID mismatch
- rejection when the requested cluster ID is unknown
- success when the image already contains the broker ID with matching metadata
- rejection when the image already contains the broker ID with conflicting
  metadata
- rejection of invalid broker endpoint metadata
- verification that `FetchMetadataImage` does not append broker registration
  records
- verification that broker restart registration does not append a duplicate
  broker registration record
- response image including brokers and partition leaders
- verification that response images are defensive copies

## Test Contracts

Startup discovery tests should use reusable test properties files. Unit tests
may use injected controller clients so retry and redirect behavior can be
verified quickly without depending on incidental map iteration, wall-clock
timing, or network availability.

Docker Compose is the primary integration test mechanism for this startup
phase. The real broker-controller and controller-controller startup flow over
the binary TCP protocol defined in [protocol.md](../protocol/protocol.md)
should be verified by launching real controller and broker processes from
fixture config files. The compose file should include `controller` and `broker`
services.

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

Random controller choice means startup should not always contact the first
endpoint in `controller.quorum.voters`. The implementation should allow tests to
inject a deterministic chooser or ordered endpoint plan so retry order can be
asserted.

Metadata image tests should use explicit cluster IDs, broker IDs, topic names,
and partition IDs. Tests must assert that callers cannot mutate stored state
through returned slices, maps, or metadata values.

## Open Items

The following items are intentionally left for later definition:

- how the leader controller is identified once Raft replaces `is.leader`
- controller-to-controller metadata-log replication transport
- topic and partition metadata mutation APIs
