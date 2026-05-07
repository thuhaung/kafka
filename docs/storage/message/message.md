# Message Design

## Purpose

The message component defines the record model exchanged between clients and
brokers and persisted inside partition logs.

At the current stage of the project, the system supports only standalone
messages. Clients send and receive one logical message at a time. Message
batching is explicitly out of scope for now and should not shape the initial
format, APIs, or validation rules beyond leaving room for future extension.

## Directory Guide

- [api.md](./api.md): validation, serialization, and helper interfaces
- [types.md](./types.md): record model, header model, storage framing, and
  recommended Go types
- [testing.md](./testing.md): validation rules, failure modes, testing
  strategy, and open questions
- [log.md](../log/log.md): append-only log behavior that persists serialized
  records

## Goals

The message design must provide:

- A stable in-memory and on-disk representation of a single record
- A storage framing that allows readers to determine record boundaries inside a
  segment
- Clear validation rules before broker append
- A serialization format that is simple to encode, decode, and evolve
- Enough metadata to support broker persistence, fetch, replication, and
  consumer delivery
- Predictable handling of optional keys and repeated headers

## Non-Goals

The initial message design does not attempt to support:

- Record batches
- Compression
- Per-message checksums if the log layer already provides integrity checks
- Schema registry integration
- Transactional producer semantics

Those features may be added later, but the first version should optimize for
clarity and correctness. The record shape includes `ProducerId` and `Sequence`
so idempotent producer behavior can be added later, but idempotency is not
implemented in this phase. The broker preserves `Sequence` as record metadata
but does not use it for deduplication, monotonicity checks, producer fencing,
transactional commits, aborts, or atomic multi-partition writes.

## Design Principles

### Broker-Owned Offset

Clients do not set offsets on produce requests. The broker assigns the offset
when the record is appended to a partition log. Consumers receive the assigned
offset on fetch.

If a client sends a record with `Offset` already populated, the broker ignores
that value and overwrites it with the assigned partition offset. This keeps
offset allocation authoritative and prevents conflicting client-generated
positions.

### Producer-Owned Sequence

Each produced record carries a producer-supplied identifier and a 32-bit
sequence number for a specific partition. These fields are included so future
idempotent produce behavior has a stable record shape.

In this phase, `Sequence` is not used by the broker. The broker does not enforce
monotonic sequence numbers and does not deduplicate records by
`ProducerId` and `Sequence`. Once idempotency is implemented, that pair should
become the partition-scoped deduplication key.

### Opaque Payload Semantics

The message layer treats key, value, and header values as opaque bytes.
Interpretation belongs to higher-level application logic or future protocol
layers.

### Explicit Nullability

The key may be null. This is distinct from an empty byte array:

- `null` key means the record has no key
- zero-length key means the record has a key whose value is empty

Header values are more flexible:

- `null` header value means the header is present without an attached byte
  payload
- zero-length header value means the header payload is intentionally empty

### Stable Wire and Storage Shape

The same logical record structure should be usable across:

- client-to-broker produce/fetch APIs
- broker replication traffic
- partition log persistence

The encoding may differ slightly by context in the future, but the logical field
layout should remain aligned.

## Message Lifecycle

### Produce Path

1. Client constructs a `Record` without an assigned offset.
2. Client sets the `ProducerId`, partition-scoped sequence number, and
   timestamp.
3. Broker validates the message fields.
4. Broker chooses the target partition.
5. Partition log assigns the next offset.
6. Broker persists the serialized record.
7. Broker returns the assigned offset to the client.

### Fetch Path

1. Consumer requests records starting at a partition offset.
2. Broker reads serialized records from the partition log.
3. Broker deserializes them into `Record` values.
4. Broker returns records including assigned offsets and timestamps.

### Replication Path

Leader-to-follower replication should carry broker-assigned offsets and the
stored timestamp so followers reproduce the same record identity and ordering as
the leader.

Replication should also preserve the producer-supplied `ProducerId` and
sequence number as record metadata. Followers must not use those fields for
deduplication in this phase.

## State Model

Messages themselves are immutable logical values once appended to the log.

Before append, a record may exist in a partially assigned state:

- offset not assigned yet
- producer identity set by producer
- sequence set by producer for the chosen partition
- timestamp provided by client
- key optionally nil

After append, the persisted message state should be treated as fixed:

- offset assigned
- producer identity preserved
- sequence preserved
- timestamp fixed
- headers preserved with unique keys
- key and value bytes not mutated in place

## Compatibility and Evolution

The first version can be unversioned if the project wants to move quickly, but
the design should anticipate change.

Two reasonable paths:

1. Keep the record body unversioned for now.
2. Add versioning later at the protocol or log envelope boundary if format
   evolution requires it.

For this repository, the current requirement is to avoid an explicit message
format version for now.

## Recommended Next Step

1. Create `internal/message`.
2. Implement `Record`, `Header`, validation, and binary encode/decode.
3. Add unit tests covering round-trip and malformed input cases.
4. Connect the record serializer to the log package once
   [log.md](../log/log.md) is implemented.
