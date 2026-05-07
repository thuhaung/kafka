# Topic APIs

## Purpose

This document defines the topic CLI surface, internal service API, create-topic
workflow, redirect semantics, and recommended package placement.

## CLI Surface

Topic operations must be exposed as a CLI program under `cmd`.

The recommended first CLI is:

- `cmd/topic`

The create command should support:

```text
topic create <name> --bootstrap-broker <host:port> --replication-factor <n> --min-insync-replicas <n> [--partitions <n>] [--retention-ms <ms>] [--retention-bytes <bytes>]
```

Example:

```text
topic create orders --bootstrap-broker localhost:9092 --partitions 3 --replication-factor 3 --min-insync-replicas 2 --retention-ms 604800000 --retention-bytes 1073741824
```

Expected CLI behavior:

- Validate flags before sending the request.
- Require exactly one bootstrap broker address.
- Default `partitions` to `1` when not provided.
- Send the create-topic request first to the bootstrap broker.
- Print a clear success message containing the topic name and stored config.
- Print actionable validation, broker, or controller errors on failure.

## Internal API Surface

The topic package should expose APIs close to:

```go
type Service interface {
	Create(CreateRequest) (Metadata, error)
	Get(name string) (Metadata, error)
}
```

The controller-facing create request should carry the topic name and config plus
caller context:

```go
type CreateTopicRequest struct {
	ClusterID string
	Topic     topic.CreateRequest
	Internal  bool
}

type CreateTopicResponse struct {
	Error      protocol.Error
	Topic      metadata.TopicMetadata
	Partitions []metadata.PartitionMetadata
}
```

`Internal` is required for system-created topics such as
`__consumer_offsets`. Public CLI requests must not set it and must not be able
to create reserved internal topic names.

## Create Workflow

The topic program should concern itself only with whether the create request
succeeds or fails and with following the redirect path needed to reach the
controller leader.

The internal details of where controller metadata is stored are defined in
[quorum.md](../../quorum/quorum.md).

### Request Payload

The create-topic request sent over the network should include:

- Topic name
- `partitions`
- `replication.factor`
- `retention.ms` if set
- `retention.bytes` if set
- `min.insync.replicas`

The create flow should be:

1. The CLI parses flags and builds the create-topic request.
2. The CLI applies default values, including `partitions = 1` when omitted.
3. The program connects to the configured bootstrap broker.
4. The program sends the create-topic request with the topic metadata.
5. If that broker is the active controller, it processes the request and returns
   success or error.
6. If that broker is not the active controller, it returns `NOT_LEADER` and the
   leader address already stored in its memory.
7. The program makes the same create-topic request to that returned leader
   address.
8. The program returns the final success or error response to the caller.

Leader-controller processing:

1. Validate the cluster ID, topic name, and topic config.
2. Reject public requests for reserved internal topic names.
3. Check the in-memory metadata image for an existing topic with the same name.
4. If the topic exists and its stored config matches the requested config,
   return the existing topic and partition metadata without appending new
   records.
5. If the topic exists with a different config, set the response error to
   `TOPIC_ALREADY_EXISTS` while still populating `Topic` and `Partitions` with
   the existing metadata.
6. Append `TopicCreationRecord` to `__cluster_metadata`.
7. Apply the topic metadata to the in-memory metadata image.
8. For each partition ID from `0` to `partitions - 1`, choose the initial
   replica assignment, append `PartitionCreationRecord`, and apply the
   partition metadata to the image.
9. Return the created topic metadata and partition metadata.

The first implementation does not handle controller crash windows between log
append and image mutation or metadata replay after restart. Those cases belong
to the later controller recovery design.

### Redirect Semantics

The client-facing redirect contract should be:

- The first request may target any reachable bootstrap broker.
- A non-controller broker does not process topic creation itself.
- A non-controller broker replies with:
  - `NOT_LEADER`
  - the current leader controller address from broker memory
- The CLI retries the same request against that leader address.

The request must be retried without changing the topic metadata payload.

### Retry Semantics

`CreateTopic` is idempotent for the same
`cluster.id + topic name + topic config + internal flag` request.

Retry behavior:

- The CLI, broker, or coordinator may retry the same `CreateTopic` after
  connection close, write timeout, or response read timeout, subject to the
  bounded retry policy in [protocol.md](../../protocol/protocol.md).
- Retried requests must preserve the same topic name, partition count,
  replication factor, retention config, min ISR, and `Internal` value.
- A retry that observes the topic already exists with the same config must be
  treated as success and must not append duplicate metadata records.
- A retry that observes the topic already exists with a different config must
  return `TOPIC_ALREADY_EXISTS` with the existing metadata so the caller can
  report the conflict clearly.
- Retrying after `NOT_LEADER` must send the unchanged request to the returned
  leader controller.
- Retrying after `LEADER_UNKNOWN` may try another configured controller endpoint
  only when the caller has controller quorum metadata.

This makes public topic creation and internal topic creation safe after an
unknown outcome. If the first attempt committed the topic metadata but lost the
response, the retry converges on the existing matching topic instead of creating
another topic or surfacing a false failure.

### Success and Error Handling

The topic program should expose only the observable outcome of the request:

- Success when the leader accepts and completes topic creation
- Success when a retry finds that the topic already exists with the same config
- `TOPIC_ALREADY_EXISTS` when the topic name is already present with a
  different config; this response must still include the existing topic metadata
  and partition metadata
- Error when validation fails
- Error when the bootstrap broker cannot be reached
- Error when a `NOT_LEADER` response does not include a usable leader address
- Error when the leader cannot be reached or rejects the request

## Package Placement

Recommended package ownership:

- `cmd/topic`: CLI entrypoint and flag handling
- `internal/topic`: topic request validation and metadata model
- `internal/protocol`: create-topic request and response types, including
  `NOT_LEADER`
- `internal/controller` or `internal/raft`: controller internals described
  separately from this topic API contract
