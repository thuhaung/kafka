# Message APIs

## Purpose

This document defines the public interfaces and helper APIs expected from the
message package.

## Public Interfaces

The message package should provide a small, explicit API surface:

```go
type Validator interface {
	ValidateRecord(record Record) error
}

func Validate(record Record) error
func Encode(record Record) ([]byte, error)
func Decode(data []byte) (Record, error)
func EncodedSize(record Record) (int, error)
```

Optional helper APIs that may be useful:

```go
func Clone(record Record) Record
func CloneHeaders(headers []Header) []Header
```

Cloning helpers are useful because slices are reference types and records may
otherwise share mutable backing arrays between clients, broker internals, and
tests.

## Broker Policy Hooks

The message format should stay simple, but the broker will likely need a few
policy decisions:

- Future idempotency policy:
  - how the broker tracks the latest accepted `(ProducerId, Sequence)` pair per
    partition stream
  - whether gaps are allowed or only monotonic increase is enforced
  - how duplicate produce attempts are detected and acknowledged
- Size policy:
  - maximum record size
  - maximum header count
  - maximum header key length
  - maximum header value size

These are policy choices layered on top of the core record structure, not
changes to the record format itself. Idempotency is not implemented in this
phase; `Sequence` is preserved as metadata but not used by the broker.
