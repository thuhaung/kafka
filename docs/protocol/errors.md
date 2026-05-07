# Protocol Error Design

## Purpose

Protocol errors define the shared error envelope used by broker, controller,
and coordinator APIs.

Individual component documents own the request and success response schemas for
their domain. This document owns the reusable error codes and redirect payload
shape so that components do not each invent their own `NOT_LEADER`,
`LEADER_UNKNOWN`, or validation error format.

The binary TCP framing and primitive encoding rules are defined in
[protocol.md](./protocol.md).

## Goals

The protocol error model must provide:

- Stable numeric error codes
- A small error envelope that can be embedded in API responses
- Controller redirect information for requests sent to a non-leader controller
- Enough context for callers to decide whether to retry, redirect, or fail

## Error Codes

Recommended first-phase error codes:

| Code | Name | Meaning |
| --- | --- | --- |
| `0` | `NONE` | Request succeeded |
| `1` | `NOT_LEADER` | The contacted node is not the leader for this request, and the leader may be known |
| `2` | `LEADER_UNKNOWN` | The contacted node cannot currently identify the leader |
| `3` | `CLUSTER_MISMATCH` | Request cluster ID does not match the node or metadata image cluster ID |
| `4` | `INVALID_REQUEST` | Request is malformed or missing required fields |
| `5` | `INVALID_BROKER_REGISTRATION` | Broker registration metadata is invalid or conflicts with existing metadata |
| `6` | `INTERNAL_ERROR` | The server failed while processing an otherwise valid request |
| `7` | `TOPIC_ALREADY_EXISTS` | Topic creation requested a name that already exists in the metadata image |

Error names should be stable once implemented. New codes may be added later,
but existing numeric meanings should not be reused.

## Endpoint Payloads

Controller redirects need a controller endpoint payload:

```go
type ControllerEndpoint struct {
	NodeID int32
	Host   string
	Port   int32
}
```

`Host` and `Port` refer to the controller listener endpoint that the caller
should use for controller-facing traffic.

## Error Envelope

Every protocol response that can fail should include an error envelope:

```go
type Error struct {
	Code             ErrorCode
	Message          string
	LeaderController *ControllerEndpoint
}
```

Field behavior:

| Field | Required | Description |
| --- | --- | --- |
| Code | Yes | Stable numeric error code |
| Message | No | Human-readable diagnostic text for logs and CLI output |
| LeaderController | No | Controller endpoint used only when the response can redirect to a known controller leader |

`Message` is diagnostic only. Clients must branch on `Code`, not string
matching against `Message`.

## Redirect Errors

### NOT_LEADER

`NOT_LEADER` means the contacted controller is not the leader for the request.

If the contacted controller knows the leader, it must include
`LeaderController`:

```go
Error{
	Code:    ErrorNotLeader,
	Message: "controller is not the leader",
	LeaderController: &ControllerEndpoint{
		NodeID: 1,
		Host:   "localhost",
		Port:   9093,
	},
}
```

Client behavior:

- Cache the returned `LeaderController` for later controller communication.
- Retry the same request against that endpoint.
- Preserve the original request cluster ID and broker ID during retry.
- If `LeaderController` is absent, treat the response like `LEADER_UNKNOWN`.

### LEADER_UNKNOWN

`LEADER_UNKNOWN` means the contacted controller cannot currently identify the
leader.

The response must not include `LeaderController`:

```go
Error{
	Code:             ErrorLeaderUnknown,
	Message:          "controller leader is unknown",
	LeaderController: nil,
}
```

Client behavior:

- Do not cache a leader endpoint.
- Try another configured controller quorum endpoint.
- Fail startup discovery only after all configured endpoints are exhausted or a
  terminal error is returned.

## Terminal Errors

These errors should not trigger controller redirect behavior:

- `CLUSTER_MISMATCH`
- `INVALID_REQUEST`
- `INVALID_BROKER_REGISTRATION`

During startup discovery, `INTERNAL_ERROR` is retryable until the configured
startup timeout is reached. Outside startup discovery, callers may retry
`INTERNAL_ERROR` only when the component document marks the operation as safe to
retry.

## Testing Strategy

Tests should cover:

- decoding and branching on each stable error code
- `NOT_LEADER` with a populated leader controller endpoint
- `LEADER_UNKNOWN` with no leader controller endpoint
- clients ignoring `Message` for control flow
- retry logic preserving the original request when redirecting
