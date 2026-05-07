# Segment Log File Design

## Purpose

The `.log` file is the primary data file inside a segment.

It stores the raw binary representation of messages for a single contiguous range of offsets within one partition. The `.index` and `.timeindex` files exist to accelerate lookup, but the `.log` file remains the source of truth for record bytes.

## Goals

The `.log` file design must provide:

- Append-only storage for partition records
- Stable ordering of records within a segment
- A filename derived from the segment base offset
- Compatibility with offset-based and timestamp-based lookup through companion index files
- Predictable rollover behavior driven by `segment.bytes` and `segment.ms`

## Non-Goals

The initial `.log` file design does not attempt to define:

- Compaction mechanics inside a segment
- Checksums or corruption markers
- Compression
- Independent rollover policies separate from the segment
- Multi-record batch encoding

Those can be added later without changing the `.log` file's role as the segment's append-only data file.

## File Identity

Each `.log` file belongs to exactly one segment and has the same base filename as that segment's `.index` and `.timeindex` files.

The filename is derived from the 64-bit base offset of the first message stored in the segment and is encoded as a zero-padded 20-digit decimal string.

Examples:

- Offset `0` becomes `00000000000000000000.log`
- Offset `42` becomes `00000000000000000042.log`
- Offset `123456` becomes `00000000000000123456.log`

This naming convention ensures lexical ordering matches numeric offset ordering.

## Directory Placement

Each `.log` file is stored inside the directory for its partition.

Example:

- `topic-0/00000000000000000000.log`

This means a partition directory contains one `.log` file per segment, alongside the segment's `.index` and `.timeindex` files.

## Responsibilities

The `.log` file is responsible for:

- Persisting serialized message bytes in append order
- Preserving the order assigned by the partition leader broker
- Holding the bytes referenced by `.index` entries
- Holding the records referenced indirectly through `.timeindex`

The `.log` file is not responsible for direct offset lookup by itself. Lookup is accelerated by `.index` and `.timeindex`, then resolved back into byte positions inside the `.log` file.

## Data Model

The `.log` file contains the raw binary data of each message written into the segment.

Messages are stored sequentially in offset order. Since offsets are assigned by the broker before persistence, the physical order in the `.log` file must match the logical order of offsets in that segment.

Within a single `.log` file:

- The first record has the segment base offset
- Each later record has a greater offset than the one before it
- No record may be inserted between already-persisted records
- No record may be overwritten in place during normal operation

## Record Framing

The `.log` file stores serialized messages as binary entries appended one after another.

To support efficient reads and recovery, each entry should be framed so readers can identify record boundaries while scanning through the file.

Each stored record should have this structure:

1. Offset: 8 bytes
2. Message length: 4 bytes
3. Serialized message body: `Message length` bytes

At minimum, this framing makes it possible to:

- Determine where a record starts
- Determine how many bytes belong to that record
- Advance to the next record without ambiguity

A reader determines the end of the current record by reading the 8-byte offset, then the message length, then advancing by the message body size. The next record begins immediately after `8 + 4 + Message length` bytes from the current record start.

The exact encoding of the message body should follow the message format defined in [message.md](../message/message.md).

## Append Semantics

Only the active segment's `.log` file may receive new data.

Append behavior follows these rules:

- The broker appends messages only at the end of the file
- Existing bytes are not rewritten during normal append flow
- Appends occur in offset order
- Appending to an inactive segment is not allowed

This keeps the `.log` file append-only and aligns with the segment model where older segments become immutable and later eligible for whole-segment deletion.

## Durability Policy

For now, fsync behavior is not configurable.

The broker should call `fsync` after every append operation to the active `.log` file. This favors correctness and simpler failure semantics over throughput.

## Active and Inactive State

The active state of a segment is determined by whether the segment has an active file handle.

For the `.log` file this means:

- If the segment has the active file handle, the `.log` file may be appended to
- If the segment does not have the active file handle, the `.log` file is read-only from the broker's perspective

There should be exactly one active `.log` file per partition at a time because there is exactly one active segment per partition.

## Size-Based Rollover

Each topic configures `segment.bytes` as the maximum allowed size of the active `.log` file.

Before appending a new message, the broker must check whether writing that message would cause the `.log` file to exceed `segment.bytes`.

If the append would exceed `segment.bytes`:

1. The current segment becomes inactive
2. A new segment is created
3. New `.log`, `.index`, and `.timeindex` files are created using the same new base filename
4. The message is appended to the new `.log` file instead

The overflowing message is never partially written to the old `.log` file.

## Time-Based Rollover

Each topic may also configure `segment.ms`.

`segment.ms` defines the maximum age of the active segment before rollover is required, but rollover is checked only when the segment receives new data.

This means:

- There is no background thread that rolls the `.log` file on its own
- The broker checks `segment.ms` during append handling
- If `segment.ms` has been exceeded when a new message arrives, the broker rolls to a new segment before writing that message

When time-based rollover occurs:

1. The current `.log` file becomes part of an inactive segment
2. A new `.log` file is created with matching new `.index` and `.timeindex` files
3. The arriving message is written to the new `.log` file

## Relationship to `.index`

The `.index` file maps message offsets to byte offsets in the `.log` file.

This means the `.log` file must support stable byte addressing for persisted records. Once a record is written, its starting byte offset must remain valid for indexed reads for as long as the segment exists.

The broker uses this relationship as follows:

1. Find the message offset in `.index`
2. Resolve that offset to a byte position in `.log`
3. Read and decode the record bytes from `.log`

## Relationship to `.timeindex`

The `.timeindex` file maps a timestamp to one or more message offsets.

The `.log` file supports timestamp lookup indirectly:

1. The broker finds offsets for the requested timestamp in `.timeindex`
2. The broker resolves those offsets into byte positions through `.index`
3. The broker reads the corresponding records from `.log`

Because multiple messages may share the same timestamp, the `.log` file must support reading multiple records associated with the same timestamp through repeated offset lookups.

## Recovery Behavior

On broker restart:

- The broker scans the partition directory and discovers all segment files
- The base offset of each `.log` file is reconstructed from its filename
- Existing open file handles are gone, so no segment is active yet in memory
- The segment with the highest base offset is selected as the active segment
- The broker assigns the active file handle to that segment's `.log` file

All other `.log` files are treated as belonging to inactive segments after recovery.

### Truncated Tail Recovery

If the broker crashes during a write, the tail of the active `.log` file may contain:

- An incomplete offset field
- A complete offset but incomplete message length field
- A complete offset and message length, but an incomplete message body

The recommended recovery behavior is:

1. Select the segment with the highest base offset as the recovered active segment
2. Sequentially scan that latest segment's `.log` file from the beginning
3. For each record, read the 8-byte offset and 4-byte message length
4. Verify that `Message length` is non-negative and that enough bytes remain in the file for the full message body
5. If the next full record is present, advance to the next record boundary
6. If the header or body is incomplete, stop scanning and treat that record as a truncated tail
7. Truncate the file back to the end of the last complete record
8. Rebuild or repair `.index` and `.timeindex` from the surviving records if needed

For now, recovery should not use checkpoints. The broker should always scan the latest segment from the beginning.

### Relationship to Index Recovery

If a record was appended durably to `.log` but the broker crashed before the corresponding `.index` or `.timeindex` entry was fully appended, recovery should treat `.log` as the source of truth.

That means:

- The broker rescans the segment `.log` file from the beginning
- The same pass that detects a truncated `.log` tail can also detect missing, stale, or partially appended index state
- `.index` should be rewritten so each recovered message offset points to the correct byte position in `.log`
- `.timeindex` should be rewritten so each recovered timestamp points to the correct message offset

### Recovery Across Rolled Segments

If `.log` rolled to a new segment but the broker crashed before creating or fully populating the matching `.index` or `.timeindex` file, recovery should not stop at checking only the newest filenames.

Instead, the broker should:

1. Check the latest `.log` file and look for its corresponding `.index` and `.timeindex`
2. If either companion file is missing, walk backward to the most recent valid index file for that partition
3. Rebuild forward from that last valid point up to and including the current active segment

This ensures index recovery covers interrupted rollover as well as interrupted append.

This approach is a good fit for the current design because:

- Record boundaries are explicit
- Integrity checks are intentionally out of scope for now
- `fsync` on every append reduces, but does not fully eliminate, the chance of a partial tail record after a crash

## Deletion Behavior

Inactive `.log` files are deleted only as part of whole-segment deletion.

The `.log` file should not be removed independently from its `.index` and `.timeindex` companions. A segment is deleted as a unit so its files always remain consistent with one another.

## Invariants

The implementation should preserve the following invariants for `.log` files:

- Each `.log` file belongs to exactly one segment
- Each `.log` file shares its base filename with one `.index` and one `.timeindex`
- The filename is the segment base offset encoded as a 20-digit zero-padded decimal string
- The first `.log` file in a partition is `00000000000000000000.log`
- Only the active `.log` file may accept appends
- Records are appended in offset order only
- Indexed byte offsets remain stable after persistence
- Inactive `.log` files may only be removed through whole-segment deletion

## Testing Focus

Tests for the `.log` file should cover at least:

- Creating the first `.log` file as `00000000000000000000.log`
- Storing `.log` files in the partition directory
- Using the same base filename for `.log`, `.index`, and `.timeindex`
- Appending records only to the active `.log` file
- Refusing appends to inactive `.log` files
- Preserving append order and offset order
- Rolling to a new `.log` file when `segment.bytes` would be exceeded
- Rolling to a new `.log` file on append when `segment.ms` has been exceeded
- Creating new companion index files on rollover
- Recovering the active `.log` file by choosing the highest-base-offset segment after restart
- Treating `.log` as the source of truth when `.index` or `.timeindex` is missing or partially appended
- Rebuilding index files forward from the last valid segment after a crash during rollover
- Reading records correctly through byte offsets supplied by `.index`

## Deferred Topics

The `.log` file design still leaves these choices for later:

- Whether integrity checks should be added in a future version
- Whether a later version should relax `fsync` on every append for better throughput
- Whether recovery should always scan from the beginning or use checkpoints to reduce restart time
