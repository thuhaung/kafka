# Controller Quorum API Design

## Purpose

The controller quorum API defines request and response schemas for
broker-to-controller and controller-to-controller startup metadata discovery.

This document owns the controller API surface. Request and response types should
live under `internal/protocol` so broker and controller packages share one wire
contract. Shared error codes and redirect payloads are defined in
[errors.md](../protocol/errors.md). Cluster metadata image fields are defined in
[cluster_metadata.md](../metadata/cluster_metadata/cluster_metadata.md).

## Directory Guide

- [quorum.md](./quorum.md): general controller quorum responsibilities,
  startup flow, temporary leader model, and metadata fetch behavior
- [types.md](./types.md): controller metadata model, server properties, Go
  types, runtime access model, and metadata image state
- [testing.md](./testing.md): validation rules, failure modes, testing strategy,
  test contracts, and open items

## Goals

The first-phase controller quorum API must provide:

- A broker startup request that registers the broker and fetches cluster
  metadata
- A controller startup request that fetches cluster metadata without registering
  a broker
- A response containing either a protocol error or a metadata image
- Redirect behavior for non-leader controllers
- Retry behavior for redirects before the leader accepts the registration
- A response shape brokers can use to build their own in-memory metadata image

## RegisterBrokerAndFetchMetadata

`RegisterBrokerAndFetchMetadata` is the first-phase startup API used by a
broker after it loads the properties file supplied by `config.path`.

The request has two jobs:

1. Register the broker in the cluster metadata log.
2. Return the current metadata image for the broker's cluster ID.

Only the leader controller may append the broker registration record. A
non-leader controller returns a redirect error from
[errors.md](../protocol/errors.md).

This phase treats broker startup registration as idempotent for broker restarts.
If the cluster metadata image already contains the broker ID for the request's
cluster ID and the stored broker metadata matches the request's advertised
broker metadata, the leader controller must not append another registration
record. It should return the current metadata image as a successful response.

If the image already contains the broker ID but the request conflicts with the
stored broker metadata, the leader controller must reject the request with
`INVALID_BROKER_REGISTRATION`.

## Request Schema

Recommended Go shape:

```go
type RegisterBrokerAndFetchMetadataRequest struct {
	ClusterID      string
	BrokerID       int32
	Host           string
	Port           int32
	ControllerHost string
	ControllerPort int32
	LogDir         string
}
```

Field meanings:

| Field | Type | Required | Description |
| --- | --- | --- | --- |
| ClusterID | string | Yes | Cluster identifier from the broker's properties file |
| BrokerID | int32 | Yes | Broker node ID from the broker's properties file |
| Host | string | Yes | Advertised broker host for client and broker traffic |
| Port | int32 | Yes | Advertised broker port for client and broker traffic |
| ControllerHost | string | No | Broker controller-listener host when different from `Host` |
| ControllerPort | int32 | No | Broker controller-listener port when different from `Port` |
| LogDir | string | No | Broker-local log directory from node configuration |

Validation rules:

- `ClusterID` must be non-empty and must match the controller's cluster ID.
- `BrokerID` must be greater than or equal to zero.
- `Host` must be non-empty.
- `Port` must be greater than zero.
- `ControllerPort`, if set, must be greater than zero.
- `ControllerHost` may be empty only when `ControllerPort` is also empty.
- The leader controller must look up the current in-memory metadata image by
  `ClusterID`.
- The image for `ClusterID` must exist before the broker can be registered. A
  leader controller with no committed metadata records should create and publish
  an empty image for its configured `ClusterID` during startup.
- The image's `ClusterID` must match the request `ClusterID`.
- If the image does not contain `BrokerID`, the leader controller may append a
  new broker registration record.
- If the image already contains `BrokerID`, the stored broker metadata must
  match `Host`, `Port`, `ControllerHost`, `ControllerPort`, and `LogDir` from
  the request for the operation to be treated as a restart.

## Response Schema

Recommended Go shape:

```go
type RegisterBrokerAndFetchMetadataResponse struct {
	Error protocol.Error
	Image *metadata.Image
}
```

Field meanings:

| Field | Required | Description |
| --- | --- | --- |
| Error | Yes | Shared protocol error envelope |
| Image | On success | Current metadata image for `ClusterID` after broker registration is committed and applied |

Response behavior:

- On success, `Error.Code` is `NONE` and `Image` is populated with a defensive
  copy of the current metadata image.
- On `NOT_LEADER`, `Image` is unset and `Error.LeaderController` should be
  populated when the contacted controller knows the leader.
- On `LEADER_UNKNOWN`, `Image` is unset and `Error.LeaderController` must be
  unset.
- On terminal validation errors, `Image` is unset.

The first implementation should return a full `metadata.Image`. Later versions
may add image deltas keyed by `AppliedOffset`, but a full image keeps broker
startup deterministic and simple.

## Leader Handling

Leader-controller behavior:

1. Validate the request.
2. Fetch the current in-memory metadata image for `ClusterID`.
3. Reject the request if the image is missing or belongs to a different cluster
   ID.
4. If the image already contains `BrokerID` and the stored broker metadata
   matches the request, return a defensive copy of the current image without
   appending a new metadata-log record.
5. If the image already contains `BrokerID` but the stored broker metadata
   conflicts with the request, return `INVALID_BROKER_REGISTRATION`.
6. If the image does not contain `BrokerID`, translate the request into a
   `BrokerRegistrationRecord`.
7. Append the record through the abstract metadata-log append/commit interface.
8. Wait until the record is committed according to the current placeholder
   metadata-log commit rules.
9. Apply the committed record to the controller's in-memory metadata image.
10. Return a defensive copy of the updated image.

Non-leader-controller behavior:

- Return `NOT_LEADER` when the leader endpoint is known.
- Return `LEADER_UNKNOWN` when the leader endpoint is not known.
- Do not append broker registration records.
- Do not return a local image as if it were the leader's current image.

## Retry Semantics

The request is safe to retry until the startup timeout is reached when:

- the previous attempt received `NOT_LEADER`
- the previous attempt received `LEADER_UNKNOWN`
- the previous attempt failed before receiving a response
- the previous attempt received `INTERNAL_ERROR`

If a response returns `NOT_LEADER` without `LeaderController`, the caller treats
it like `LEADER_UNKNOWN` and tries another configured controller endpoint.

The request is not a broker metadata update operation. If the leader has already
committed and applied a registration for the same broker ID, a repeated request
with identical endpoint and log-directory metadata is treated as a broker
restart and returns the current image. A repeated request with conflicting
metadata must return `INVALID_BROKER_REGISTRATION`. Later broker heartbeat,
controlled shutdown, fencing, or update records can relax this rule
deliberately.

## FetchMetadataImage

`FetchMetadataImage` is the first-phase startup API used by a non-leader
controller to fetch the current metadata image from the leader controller.

It is read-only. It must not register a broker, append a
`BrokerRegistrationRecord`, or mutate the metadata image. Its purpose is to let
a non-leader controller recover or refresh its local read-only controller state
from the leader.

## FetchMetadataImage Request Schema

Recommended Go shape:

```go
type FetchMetadataImageRequest struct {
	ClusterID string
	NodeID    int32
}
```

Field meanings:

| Field | Type | Required | Description |
| --- | --- | --- | --- |
| ClusterID | string | Yes | Cluster identifier from the controller's properties file |
| NodeID | int32 | Yes | Controller node ID from the controller's properties file |

Validation rules:

- `ClusterID` must be non-empty and must match the controller's cluster ID.
- `NodeID` must be greater than or equal to zero.
- The leader controller must look up the current in-memory metadata image by
  `ClusterID`.
- The image for `ClusterID` must exist. A leader controller with no committed
  metadata records should create and publish an empty image for its configured
  `ClusterID` during startup.
- The image's `ClusterID` must match the request `ClusterID`.

## FetchMetadataImage Response Schema

Recommended Go shape:

```go
type FetchMetadataImageResponse struct {
	Error protocol.Error
	Image *metadata.Image
}
```

Response behavior:

- On success, `Error.Code` is `NONE` and `Image` is populated with a defensive
  copy of the current metadata image.
- On `NOT_LEADER`, `Image` is unset and `Error.LeaderController` should be
  populated when the contacted controller knows the leader.
- On `LEADER_UNKNOWN`, `Image` is unset and `Error.LeaderController` must be
  unset.
- On terminal validation errors, `Image` is unset.

Leader-controller behavior:

1. Validate the request.
2. Fetch the current in-memory metadata image for `ClusterID`.
3. Return a defensive copy of the image without appending any metadata-log
   records.

Non-leader-controller behavior:

- Return `NOT_LEADER` when the leader endpoint is known.
- Return `LEADER_UNKNOWN` when the leader endpoint is not known.
- Do not return a local image as if it were the leader's current image.

## Wire Encoding

The first implementation must expose `RegisterBrokerAndFetchMetadata` and
`FetchMetadataImage` over the binary TCP protocol defined in
[protocol.md](../protocol/protocol.md). The logical wire schema must preserve:

- the shared request header with `APIKey` and `CorrelationID`
- the shared response header with the matching `CorrelationID`
- stable field names or field IDs
- signed integer widths shown in the request schema
- a nullable image in the response
- a shared `protocol.Error` response envelope

String fields should use length-prefixed byte sequences and numeric fields
should use the network byte order defined by the protocol document.

Startup API keys:

| API | APIKey |
| --- | --- |
| `RegisterBrokerAndFetchMetadata` | `1` |
| `FetchMetadataImage` | `2` |

Controllers must support multiple in-flight startup requests on a single TCP
connection. Each response must copy the request's `CorrelationID` so brokers and
controllers can match responses even when several brokers register at the same
time.
