# Message Testing

## Purpose

This document defines validation rules, failure modes, test expectations, and
known unresolved design issues for the message component.

## Validation Rules

The broker should validate records before append using rules like the following:

- `Value` must not be nil.
- `Timestamp` must be present.
- `ProducerId` must be present and valid.
- `Sequence` must be present and valid as metadata, but the broker must not use
  it for monotonicity checks or deduplication in this phase.
- `Offset` supplied by a producer must be ignored and overwritten by the broker.
- `Headers` count must be within configured limits.
- Each header key must be non-empty.
- Header keys must be valid UTF-8 strings.
- Header keys must be unique when the broker receives the record.
- Total serialized message size must be within broker limits.
- Individual header values must be within broker limits.

The message package should own structural validation, while size-policy
validation may be shared with the broker or protocol package if limits are
configurable.

## Failure Modes

The message component should explicitly handle:

- Decode failure due to truncated bytes.
- Decode failure due to invalid negative lengths.
- Decode failure due to malformed UTF-8 header keys.
- Validation failure due to nil value.
- Validation failure due to oversized record or headers.
- Serialization failure if timestamp cannot be normalized.

The caller should be able to distinguish malformed data from policy violations.
That will help the broker decide whether to reject a client request, mark a
segment as corrupt, or retry a replication fetch.

## Testing Strategy

The message component should include unit tests for:

- encode/decode round-trip for records without keys
- encode/decode round-trip for records with keys
- encode/decode round-trip preserving producer ID
- encode/decode round-trip preserving sequence number
- validation accepts repeated `(ProducerId, Sequence)` pairs in this phase
- validation does not reject sequence gaps or non-monotonic sequences in this
  phase
- encode/decode round-trip for empty values
- encode/decode round-trip for multiple headers
- encode/decode round-trip for null header values
- distinction between null key and empty key
- distinction between null header value and empty header value
- validation rejection for nil value
- validation rejection for empty header key
- validation rejection for invalid UTF-8 header key
- client-side duplicate-header override behavior
- validation rejection for duplicate header keys that still reach the broker
- decode rejection for truncated input
- decode rejection for negative lengths other than supported null sentinels
- encoded size calculation matching actual encoded output

Failure-oriented tests are especially important because malformed bytes and
boundary conditions are common sources of storage and replication bugs.

## Open Questions

The current design is sufficient for the described scope, but one decision
should be confirmed before implementing idempotency:

- Should sequence monotonicity be strictly contiguous, or only increasing?
