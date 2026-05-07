# Message Types

## Purpose

This document defines the message record model, storage encoding, and
recommended Go types for `internal/message`.

## Record Model

Each message is a single record with the following fields:

| Field | Type | Required | Description |
| --- | --- | --- | --- |
| Offset | signed 64-bit integer | Assigned by broker/log | Monotonic identifier within a partition log |
| ProducerId | signed 64-bit integer | Yes on produce | Producer identity reserved for future idempotent produce behavior. Preserved as metadata but not used for deduplication in this phase |
| Sequence | unsigned or signed 32-bit integer | Yes on produce | Producer-supplied sequence number reserved for future idempotent produce behavior. Preserved as metadata but not used for monotonicity checks or deduplication in this phase |
| Timestamp | timestamp | Yes | Client-supplied event timestamp |
| Key | byte array | No | Optional key used for routing or application semantics. Future compaction-related use is deferred because log compaction is out of scope for now |
| Value | byte array | Yes | Opaque record payload |
| Headers | array of headers | Yes, may be empty | Additional metadata attached to the record |

## Header Model

Each header contains:

| Field | Type | Required | Description |
| --- | --- | --- | --- |
| Key | string | Yes | Header name |
| Value | byte array | No | Optional opaque header payload |

Header keys must be unique within a message. Duplicate-key override happens on
the client side: if a client adds a header whose key already exists, the new
value replaces the existing value for that key before the record is sent.
Header values may be null, empty, or non-empty.

## Proposed Go Types

The exact package path may be refined during implementation, but the message
package should expose types close to the following:

```go
package message

import "time"

type Record struct {
	Offset     int64
	ProducerId int64
	Sequence   uint32
	Timestamp  time.Time
	Key        []byte
	Value      []byte
	Headers    []Header
}

type Header struct {
	Key   string
	Value []byte
}
```

Implementation notes:

- `nil` `Key` represents a null key.
- `Value` must be non-nil after validation.
- `nil` header `Value` represents a null header value.
- `Headers` should decode as an empty slice when no headers are present.
- `ProducerId` must be set by the producer before a broker accepts the record.
  It is preserved for future idempotent produce behavior but is not used for
  deduplication in this phase.
- `Sequence` must be set by the producer before a broker accepts the record. It
  is preserved for future idempotent produce behavior but is not used for
  monotonicity checks or deduplication in this phase.
- `Offset` may be unset in client-side records before broker append, but must be
  populated before persistence and fetch response.

## Serialization Design

### Logical Encoding

The initial serializer should use a length-delimited binary format that is easy
to reason about and test.

Recommended stored record layout:

1. Offset: 8 bytes
2. Message length: 4 bytes
3. ProducerId: 8 bytes
4. Sequence: 4 bytes
5. Timestamp: 8 bytes
6. Key length: 4 bytes
7. Key bytes: variable length if present
8. Value length: 4 bytes
9. Value bytes: variable length
10. Header count: 4 bytes
11. Repeated headers:
    - Header key length: 4 bytes
    - Header key bytes: variable length
    - Header value length: 4 bytes
    - Header value bytes: variable length

`Message length` is the number of bytes occupied by the serialized message body
after the length field. In other words, a reader can determine the end of the
current stored record from:

- `Offset`: 8 bytes
- `Message length`: 4 bytes
- `Message body`: `Message length` bytes

This means the next record starts immediately after
`8 + 4 + Message length` bytes from the current record start.

### Length Rules

- A key length of `-1` represents a null key.
- A key length of `0` represents an empty key.
- Value length must be `>= 0`.
- Header count must be `>= 0`.
- Header key length must be `> 0`.
- A header value length of `-1` represents a null header value.
- A header value length of `0` represents an empty header value.
- Other header value lengths must be `>= 0`.

### Timestamp Encoding

Timestamp should be serialized as a 64-bit integer. The recommended
representation is Unix time in milliseconds since epoch because it is compact,
language-neutral, and close to Kafka conventions.

The in-memory Go type can remain `time.Time`, with serializer helpers
converting to and from epoch milliseconds.

### Byte Order

The project should choose one byte order and keep it consistent everywhere.
Big-endian is a reasonable default because it is common in network protocols and
easier to inspect in debugging tools.

### Framing

If the log subsystem stores multiple records sequentially in a segment, each
stored record should begin with:

1. The assigned `Offset`
2. A `Message length` field
3. The serialized message body

This framing allows readers to skip, scan, and recover boundaries efficiently.

The logical message body therefore begins after the `Offset` and
`Message length` fields, while the `.log` file can still use the full stored
record framing for sequential reads and recovery.
