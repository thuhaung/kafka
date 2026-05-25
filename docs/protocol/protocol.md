# Binary TCP Protocol Design

## Purpose

The protocol component defines the shared network contract used by every
process-to-process connection in the cluster.

All broker-client, broker-broker, broker-controller, and
controller-controller communication uses TCP connections between configured
hosts and ports. After a connection is established, peers exchange length
prefixed binary frames. Domain-specific component documents own the request and
response payload schemas, while this document owns the transport, framing,
primitive encoding, and connection lifecycle rules.

## Goals

The wire protocol must provide:

- a single TCP framing model for all connection types
- deterministic binary encoding for requests and responses
- shared validation and failure behavior for malformed frames
- clear listener usage for client, broker, and controller traffic

## Non-Goals

The first protocol phase does not define:

- TLS or encrypted transport
- SASL, authentication, authorization, or ACLs
- compression
- transactions
- idempotent producer sequence handling
- schema negotiation
- frame magic bytes
- frame flags
- a public compatibility promise with Apache Kafka clients

The project intentionally follows Kafka's simple TCP and binary-frame shape,
but it is not required to be wire-compatible with Apache Kafka.

## Connection Types

Every connection uses the same frame format. The listener selected for the
connection depends on the peer relationship.

| Connection | Initiator | Listener | Purpose |
| --- | --- | --- | --- |
| broker-client | producer, consumer, topic CLI, admin CLI | broker configured listener alias mapped to `PLAINTEXT` | produce, fetch, topic operations, coordinator lookup, group APIs |
| broker-broker | broker | peer broker configured listener alias mapped to `PLAINTEXT` | partition replication, follower fetch, broker metadata exchange |
| broker-controller | broker | controller configured listener alias mapped to `PLAINTEXT` | broker registration, metadata fetch, controller-directed broker requests |
| controller-controller | controller | peer controller configured listener alias mapped to `PLAINTEXT` | quorum metadata fetch, leader redirects, future Raft traffic |

In this phase, listener names are aliases chosen in configuration. The only
supported security protocol is `PLAINTEXT`, and every configured listener alias
must map to it. It carries unencrypted TCP traffic unless a later security
design adds transport protection.

## TCP Behavior

Peers open a TCP connection to the target host and port advertised in broker or
controller metadata.

Connection rules:

- Connections should be long-lived and reused for multiple requests.
- A client may have more than one connection to the same peer.
- Either peer may close an idle, invalid, or failed connection.
- Requesters must treat connection close before a complete response as an
  unknown request outcome unless the component document says the operation
  is safe to retry.
- Servers must not depend on TCP packet boundaries. They must reconstruct
  complete protocol frames from the byte stream.
- A requester may send multiple requests on the same connection without waiting
  for earlier responses.
- Every request frame must include a requester-assigned `CorrelationID`.
- Every response frame must include the same `CorrelationID` as the request it
  answers.
- Requesters must match responses to in-flight requests by `CorrelationID`, not
  by response order.
- Servers may process requests concurrently or in receive order, but responses
  may be written in any order as long as each response preserves the request's
  `CorrelationID`.
- A connection must not have two in-flight requests with the same
  `CorrelationID`.

Recommended first-phase implementation:

- use one goroutine to read frames from each accepted connection
- dispatch decoded requests to request handlers after decoding the shared
  header
- allow clients to expose a request dispatcher keyed by `ApiKey`, so
  component-specific send paths can share one raw TCP client while still
  centralizing per-request send behavior
- use a serialized writer per connection so response bytes are not interleaved
- track in-flight requests by `CorrelationID` on clients
- set read limits before allocating payload buffers

## Timeouts and Deadlines

TCP connections must not be allowed to block request progress forever. Every
network caller should use explicit deadlines for connection setup, frame writes,
response reads, and idle connection reuse.

Recommended first-phase startup defaults:

| Timeout | Default | Applies To |
| --- | --- | --- |
| connect timeout | 10 seconds | Opening a TCP connection to a broker or controller during startup |
| write timeout | 10 seconds | Writing one complete startup request frame |
| response read timeout | 10 seconds | Waiting for one complete startup response frame after the request frame is written |
| startup API timeout | 10 seconds | Completing one startup API call, including connect, write, and read |
| idle timeout | 30 seconds | Closing an otherwise healthy reused connection after no reads or writes |

These `10s` defaults apply to broker-controller and controller-controller
startup communication. Later runtime APIs may define different operation-level
timeouts when their behavior is specified.

Timeout behavior:

- If the connect timeout expires before a connection is established, no request
  was sent. The requester may try another eligible endpoint when the component
  document allows endpoint discovery or bounded retries.
- If the write timeout expires, the requester must close the connection. The
  request outcome is unknown unless the requester can prove no request bytes
  were written.
- If the response read timeout expires before a complete response frame is
  decoded, the requester must close the connection and treat the request
  outcome as unknown.
- If the idle timeout expires, either peer may close the connection. Closing an
  idle connection does not imply request failure because no request is in
  flight.
- Servers may apply read deadlines while reading frame length and frame data to
  protect against stalled peers. A server should close the connection when a
  peer times out mid-frame.
- Timeout errors must be reported separately from decoded protocol errors. A
  timeout does not carry a protocol error code because no valid response was
  received.

## Frame Format

Each frame starts with a 32-bit unsigned length followed by exactly that many
bytes of frame data.

```text
+----------------+------------------+
| Length uint32  | FrameData bytes  |
+----------------+------------------+
```

`Length` is encoded in network byte order and does not include the four bytes
used by the length field itself.

`FrameData` starts with a shared protocol header followed by the request or
response body owned by the component API document.

Shared request header:

```text
+----------------+--------------------+----------------+
| ApiKey int8    | CorrelationID int32 | Body bytes     |
+----------------+--------------------+----------------+
```

Shared response header:

```text
+--------------------+----------------+
| CorrelationID int32 | Body bytes     |
+--------------------+----------------+
```

`ApiKey` identifies the request type for dispatch on the receiving listener.
`CorrelationID` is chosen by the requester and must be copied unchanged into the
matching response. `Body` is the component-specific request or response payload.

Component API documents define their own request and response body fields.
Listeners that support multiple APIs should dispatch by `ApiKey`.

Validation rules:

- `Length` must be greater than zero unless the component document explicitly
  allows an empty payload.
- `Length` must not exceed the configured maximum frame size.
- Request frames must contain a complete shared request header.
- Response frames must contain a complete shared response header.
- `ApiKey` must be known for the receiving listener.
- `CorrelationID` must be greater than zero.
- `FrameData` body must decode according to the component API document for the
  receiving listener and request type.

If the receiver cannot decode the payload enough to produce a protocol error
response, it should close the connection.

## Primitive Encoding

All multi-byte integers use network byte order, also known as big endian.

Recommended primitive encodings:

| Type | Encoding |
| --- | --- |
| bool | one byte, `0` false and `1` true |
| int8 / uint8 | one byte |
| int16 / uint16 | two bytes, big endian |
| int32 / uint32 | four bytes, big endian |
| int64 / uint64 | eight bytes, big endian |
| timestamp | int64 Unix milliseconds |
| uuid | 16 raw bytes |
| string | int16 byte length followed by UTF-8 bytes; `-1` means null only when the field is nullable |
| bytes | int32 byte length followed by raw bytes; `-1` means null only when the field is nullable |
| array | int32 item count followed by each item; `-1` means null only when the field is nullable |

Strings are length-prefixed by byte count, not rune count. Receivers must reject
strings that are not valid UTF-8 when the field represents user-facing text,
topic names, group IDs, listener names, or hostnames.

Maps should be encoded as arrays of key-value structs. Component documents
must define whether map key order matters. When deterministic output is needed,
encoders should sort map keys lexicographically before writing the array.

## Component API Documents

This protocol document does not duplicate every component request or response
schema. Each component document owns its own APIs and payload schemas.

Component responsibilities:

- protocol framing and primitive encodings live in this document
- shared request and response headers live in this document
- shared error envelopes live in [errors.md](./errors.md)
- startup controller request and response payloads live in
  [quorum/api.md](../quorum/api.md)
- group coordinator request and response payloads live in
  [group_coordinator.md](../group_coordinator/group_coordinator.md)
- message and record field layout lives in [message.md](../storage/message/message.md)
- future produce, fetch, replication, topic, and heartbeat documents own their
  request and response structs

Any component API that needs to contact another client, broker, or controller
should use this document for the connection and framing rules, then encode the
request payload defined by the target component's API document.

All response payloads that can fail should include the shared protocol error
envelope. A successful response uses error code `NONE`.

## Error Handling

Protocol-level errors should use the shared error model from
[errors.md](./errors.md).

Malformed frame behavior:

- If the receiver can identify the request and decode enough of the payload to
  respond, it should send `INVALID_REQUEST` when the payload is malformed.
- If the receiver cannot safely decode the payload, it should close the
  connection.
- If a frame exceeds the maximum configured size, the receiver must close the
  connection and may log the remote address.
- The receiver must not keep reading a connection after losing frame alignment.

Request handler behavior:

- validation failures return `INVALID_REQUEST` unless a more specific code
  applies
- redirects use `NOT_LEADER` or `LEADER_UNKNOWN`
- unexpected server failures return `INTERNAL_ERROR`
- callers must branch on numeric error codes, not diagnostic messages

## Maximum Sizes

The implementation should define conservative limits before allocating buffers.

Recommended first-phase defaults:

| Limit | Default | Notes |
| --- | --- | --- |
| maximum frame size | 64 MiB | Applies to the full `FrameData` length |
| maximum string length | 32 KiB | Applies to generic protocol strings |
| maximum array item count | payload-specific | Validate before allocation |

Produce and fetch APIs may later define larger record-batch limits, but the
server must always enforce a configured upper bound.

## Connection Lifecycle

Recommended requester flow:

1. Resolve target host and port from bootstrap configuration or metadata.
2. Open a TCP connection.
3. Allocate a positive `CorrelationID` that is not already in flight on this
   connection.
4. Encode one request frame with the shared request header and target
   component's request body.
5. Record the in-flight request by `CorrelationID`.
6. Write the full frame.
7. Continue writing additional requests when needed, each with a distinct
   in-flight `CorrelationID`.
8. Read response frames.
9. Decode each response header and use `CorrelationID` to find the matching
   in-flight request.
10. Decode the response body according to the target component's API document.
11. Complete and remove the matching in-flight request.
12. Reuse the connection for later requests when possible.

Recommended server flow:

1. Accept the TCP connection.
2. Read the four-byte `Length`.
3. Reject the connection if the length exceeds the configured maximum.
4. Read exactly `Length` bytes.
5. Decode the shared request header.
6. Reject or close the connection if `ApiKey` is unknown for the listener or
   `CorrelationID` is invalid.
7. Dispatch the request body to the appropriate handler.
8. Encode exactly one response frame with the same `CorrelationID`.
9. Continue reading the next frame until either peer closes the connection.

## Retries

Transport retries are request-specific because retry safety depends on whether
the server may already have applied the request.

Generic rules:

- Retrying after `NOT_LEADER` is allowed when the component document defines
  redirect behavior.
- Retrying after `LEADER_UNKNOWN` is allowed against another configured peer
  when the component document defines discovery behavior.
- Retrying after connection close, write timeout, or response read timeout
  before a response is safe only for requests marked idempotent or explicitly
  retryable.
- Producers and brokers should use bounded retry loops with clear errors when
  all candidate peers fail.
- A bounded retry loop should use at most three attempts by default, including
  the first attempt.
- Retries should use short backoff delays. The first-phase default is 100 ms
  before the first retry and 500 ms before the second retry.
- Retried requests must preserve the original request payload unless the
  component document explicitly says to refresh metadata or rebuild the request.
- A requester must not retry a non-idempotent request after an unknown outcome.
  It should return a clear timeout, connection, or unknown-outcome error to the
  caller instead.

Component API documents should classify each request as one of:

| Classification | Meaning |
| --- | --- |
| idempotent | Repeating the same request has the same observable result as applying it once |
| explicitly retryable | Repeating the same request is accepted by that component's failure model even if it may perform extra work |
| not retryable after unknown outcome | The requester must not automatically retry after bytes may have reached the server |

## Testing Strategy

Protocol implementation tests should cover:

- frame length encoding and decoding
- big-endian primitive encoding
- request and response round trips over TCP
- multiple in-flight requests on one connection
- response matching by `CorrelationID` when responses are returned out of order
- rejection of duplicate in-flight `CorrelationID` values on one connection
- rejection of unknown `ApiKey` values
- maximum frame size enforcement
- connection close on incomplete or over-sized frames
- connect, write, response read, and idle timeout behavior
- retry exhaustion after the default bounded attempts
- use of the shared error envelope for decodable request failures
- successful `RegisterBrokerAndFetchMetadata` framing over a TCP connection
- successful consumer coordinator request framing over a TCP connection
