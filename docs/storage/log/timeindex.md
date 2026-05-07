# Segment Time Index File Design

## Purpose

The `.timeindex` file is the timestamp lookup structure for a segment.

It maps message timestamps to message offsets so the broker can resolve timestamp-based fetch requests without scanning the full `.log` file sequentially.

## Goals

The `.timeindex` file design must provide:

- Fast lookup from timestamp to message offset
- A stable relationship with the segment's `.log` and `.index` files
- A filename derived from the segment base offset
- Append-compatible behavior while the segment is active
- Recovery behavior that can rebuild or validate the time index after a crash

## Non-Goals

The initial `.timeindex` file design does not attempt to define:

- Independent rollover separate from the segment
- Grouping multiple offsets under one timestamp
- Checksums or integrity markers
- Advanced search structures beyond a simple appended index

Those can be added later if timestamp-based fetch needs more sophistication.

## File Identity

Each `.timeindex` file belongs to exactly one segment and shares the same base filename as that segment's `.log` and `.index` files.

The filename is derived from the 64-bit base offset of the segment and encoded as a zero-padded 20-digit decimal string.

Examples:

- Offset `0` becomes `00000000000000000000.timeindex`
- Offset `42` becomes `00000000000000000042.timeindex`
- Offset `123456` becomes `00000000000000123456.timeindex`

## Directory Placement

Each `.timeindex` file is stored inside the directory for its partition.

Example:

- `topic-0/00000000000000000000.timeindex`

This means a partition directory contains one `.timeindex` file per segment, alongside the corresponding `.log` and `.index` files.

## Responsibilities

The `.timeindex` file is responsible for:

- Mapping message timestamps to message offsets
- Narrowing timestamp-based reads to a message offset before consulting `.index`
- Supporting broker fetch by timestamp

The `.timeindex` file is not the source of truth for record bytes and does not point directly into `.log`. It resolves timestamps to offsets, and `.index` completes the lookup to byte positions.

## Data Model

Each `.timeindex` entry associates:

- A message timestamp
- A message offset

For now, the design ignores duplicate timestamps. The `.timeindex` file therefore assumes one timestamp-to-offset entry per indexed record, and duplicate-timestamp handling is deferred to a later version.

## Entry Layout

For now, each `.timeindex` entry should store:

1. Timestamp: 8 bytes
2. Message offset: 8 bytes

This simple fixed-width layout is easy to append, scan, and rebuild during recovery.

## Lookup Semantics

When the broker receives a fetch request for a timestamp:

1. It locates the segment whose time range may contain the requested timestamp
2. It consults that segment's `.timeindex` file
3. It finds the offset associated with the requested timestamp
4. It consults the segment's `.index` file to map that offset to a byte position in `.log`
5. It reads and decodes the record from `.log`

This keeps timestamp-based fetch as a two-step process:

- Timestamp to offset through `.timeindex`
- Offset to byte position through `.index`

## Write Semantics

While a segment is active, the broker appends new `.timeindex` entries as new messages are appended to the `.log` file.

Append behavior follows these rules:

- Only the active segment's `.timeindex` file may receive new entries
- New entries are appended in the same order records are appended to `.log`
- Existing entries are not rewritten during normal append flow
- Writing to an inactive segment's `.timeindex` file is not allowed

Each appended `.timeindex` entry must correspond to a committed record in the active `.log` file.

## Relationship to `.log`

The `.timeindex` file depends on the `.log` file because the record timestamp originates from the stored message.

For each appended record:

1. The broker appends the record to `.log`
2. The broker reads the record timestamp and assigned offset from that message
3. The broker appends a `.timeindex` entry mapping the timestamp to the offset

The `.timeindex` file never points directly into `.log`. It relies on `.index` for the final hop to the byte position.

## Relationship to `.index`

The `.timeindex` file works together with `.index` during timestamp-based fetch.

For timestamp-based lookup:

1. `.timeindex` resolves a timestamp to an offset
2. `.index` resolves that offset to a byte position
3. `.log` provides the record bytes at that position

This separation keeps timestamp lookup and byte-position lookup independent but composable.

## Duplicate Timestamps

For now, duplicate timestamps are out of scope.

The implementation should not attempt to group multiple offsets under the same timestamp in `.timeindex`, and the exact handling of timestamp collisions is deferred to a later version.

## Active and Inactive State

The active state of the segment is determined by whether the segment has an active file handle.

For the `.timeindex` file this means:

- If the segment is active, the `.timeindex` file may receive appended entries
- If the segment is inactive, the `.timeindex` file becomes read-only from the broker's perspective

There should be exactly one active `.timeindex` file per partition at a time because there is exactly one active segment per partition.

## Rollover Behavior

The `.timeindex` file does not roll independently.

When the segment rolls because `segment.bytes` or `segment.ms` is exceeded:

1. The current segment becomes inactive
2. A new segment is created
3. A new `.log`, `.index`, and `.timeindex` file are created together with the same new base filename
4. Future entries are appended only to the new segment's `.timeindex` file

## Recovery Behavior

On broker restart:

- The broker scans the partition directory and discovers segment files
- The base offset of each `.timeindex` file is reconstructed from its filename
- The segment with the highest base offset is selected as the recovered active segment
- The broker assigns the active file handle to that segment

Because the `.timeindex` file is derived from `.log`, recovery may rebuild or validate it from the `.log` file if needed.

### Rebuild Strategy

For now, the simplest recovery strategy is:

1. Scan the segment's `.log` file from the beginning
2. For each complete record, decode the record timestamp and record offset
3. Recreate `.timeindex` entries from those observed values
4. Truncate or rewrite the `.timeindex` file if it contains entries that do not correspond to valid records in `.log`

This keeps the `.timeindex` file recoverable even if the broker crashes after writing `.log` but before fully updating `.timeindex`.

If the broker appended the record to `.log` but only partially appended the corresponding `.timeindex` entry, recovery should reuse the same scan-from-beginning process used for truncated `.log` handling. When a partial or stale `.timeindex` entry is found, the broker should rewrite the recovered entry so it points to the correct message offset from `.log`.

If segment rollover completed for `.log` but the broker crashed before the new `.timeindex` file was created or fully initialized, recovery should:

1. Check the latest `.log` file for its matching `.timeindex`
2. If the file is missing or invalid, locate the last valid `.timeindex` file before it
3. Rebuild `.timeindex` files from that point forward through the current active segment

This lets time-index recovery handle both interrupted append and interrupted rollover while treating `.log` as the authoritative record source.

## Deletion Behavior

Inactive `.timeindex` files are deleted only as part of whole-segment deletion.

The `.timeindex` file should not be removed independently from its `.log` and `.index` companions. A segment is deleted as a unit so the three files stay consistent with one another.

## Invariants

The implementation should preserve the following invariants for `.timeindex` files:

- Each `.timeindex` file belongs to exactly one segment
- Each `.timeindex` file shares its base filename with one `.log` and one `.index`
- The filename is the segment base offset encoded as a 20-digit zero-padded decimal string
- The first `.timeindex` file in a partition is `00000000000000000000.timeindex`
- Only the active `.timeindex` file may accept appended entries
- Entries are appended in the same order as records are appended to `.log`
- Each `.timeindex` entry maps one timestamp to one offset for now
- Inactive `.timeindex` files may only be removed through whole-segment deletion

## Testing Focus

Tests for the `.timeindex` file should cover at least:

- Creating the first `.timeindex` file as `00000000000000000000.timeindex`
- Storing `.timeindex` files in the partition directory
- Using the same base filename for `.timeindex`, `.log`, and `.index`
- Appending entries only to the active `.timeindex` file
- Refusing writes to inactive `.timeindex` files
- Mapping timestamps to the correct message offsets
- Looking up records by timestamp through `.timeindex`, `.index`, and `.log`
- Creating a new `.timeindex` file when the segment rolls
- Rebuilding `.timeindex` from `.log` during recovery
- Ignoring duplicate-timestamp grouping for now
- Rewriting partially appended `.timeindex` entries to the correct offsets during recovery
- Rebuilding missing `.timeindex` files after a crash that interrupted segment rollover

## Deferred Topics

The `.timeindex` file design still leaves these choices for later:

- How duplicate timestamps should be handled
- Whether timestamp lookups should support nearest-greater-than or nearest-less-than semantics rather than exact-match only
- Whether time-index rebuild should always rewrite the file or only validate and truncate when needed
