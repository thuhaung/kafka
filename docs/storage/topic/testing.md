# Topic Testing

## Purpose

This document defines validation rules, failure modes, test expectations, and
deferred work for the topic component.

## Validation Rules

### Topic Name

The topic name must satisfy these initial rules:

- Must not be empty.
- Must be at most 249 characters.
- Must contain only ASCII letters, digits, `.`, `_`, and `-`.
- Must not be `.` or `..`.
- Must be unique across existing topic metadata entries in the controller
  quorum.
- Must not use a reserved internal topic name such as `__consumer_offsets` or
  `__cluster_metadata` unless the caller is cluster bootstrap code.

### Topic Config

The topic config must satisfy:

- `partitions` must be greater than `0`.
- `replication.factor` must be greater than `0`.
- `min.insync.replicas` must be greater than `0`.
- `min.insync.replicas` must be less than or equal to `replication.factor`.
- `retention.ms`, if set, must be greater than or equal to `-1`.
- `retention.bytes`, if set, must be greater than or equal to `-1`.

The value `-1` for retention fields means unlimited retention for that
dimension.

## Failure Modes

The topic component should explicitly handle:

- Duplicate topic creation requests
- Unreachable bootstrap broker
- `NOT_LEADER` responses with missing or stale leader addresses
- Invalid config combinations such as
  `min.insync.replicas > replication.factor`
- Leader change between the first redirect and the retried request
- Broker restart with stale in-memory leader metadata

Recommended behavior:

- If the bootstrap broker is unreachable, return a connection error
  immediately.
- If the redirected leader cannot be reached, return an error rather than
  silently succeeding.
- If the leader changes between redirect and retry, the program may receive
  another redirect-style error and should surface that failure clearly.
- If the request was committed but the client did not receive the response, a
  repeated create for the same name should return the already-created metadata
  or a deterministic already-exists error.

## Testing Focus

Tests for the topic component should cover at least:

- Creating a topic through the CLI with valid config
- Defaulting `partitions` to `1` when omitted
- Rejecting `partitions <= 0`
- Rejecting a missing `--bootstrap-broker`
- Rejecting invalid topic names
- Rejecting public create requests for reserved internal topic names
- Rejecting `replication.factor <= 0`
- Rejecting `min.insync.replicas <= 0`
- Rejecting `min.insync.replicas > replication.factor`
- Accepting unlimited retention via `-1`
- Sending the first create-topic request to the bootstrap broker
- Retrying the same request to the returned leader on `NOT_LEADER`
- Returning an error when the redirect response omits the leader address
- Handling duplicate create requests deterministically

## Deferred Topics

The topic design intentionally leaves these choices for later:

- Topic config updates after creation
- Topic deletion records and tombstone semantics
