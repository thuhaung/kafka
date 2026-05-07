# Segment Index File Design

## Purpose

The `.index` file is the offset lookup structure for a segment.

It maps message offsets to byte offsets inside the segment's `.log` file so the broker can locate records efficiently without scanning the full `.log` file from the beginning.

## Goals

The `.index` file design must provide:

- Fast lookup from message offset to byte position in the `.log` file
- A stable relationship with the segment's `.log` file
- A filename derived from the segment base offset
- Append-compatible behavior while the segment is active
- Recovery behavior that can rebuild or validate the index after a crash

## Non-Goals

The initial `.index` file design does not attempt to define:

- Independent rollover separate from the segment
- Compaction of index entries
- Checksums or integrity markers
- Multi-level or tree-based indexing

Those can be added later if the segment implementation needs more sophisticated read performance.

## File Identity

Each `.index` file belongs to exactly one segment and shares the same base filename as that segment's `.log` and `.timeindex` files.

The filename is derived from the 64-bit base offset of the segment and encoded as a zero-padded 20-digit decimal string.

Examples:

- Offset `0` becomes `00000000000000000000.index`
- Offset `42` becomes `00000000000000000042.index`
- Offset `123456` becomes `00000000000000123456.index`

## Directory Placement

Each `.index` file is stored inside the directory for its partition.

Example:

- `topic-0/00000000000000000000.index`

This means a partition directory contains one `.index` file per segment, alongside the corresponding `.log` and `.timeindex` files.

## Responsibilities

The `.index` file is responsible for:

- Mapping message offsets to byte offsets in the segment's `.log` file
- Narrowing fetch reads to the correct position in the `.log` file
- Supporting efficient offset-based reads for the broker

The `.index` file is not the source of truth for record bytes. It is a lookup aid layered on top of the `.log` file.

## Data Model

Each `.index` entry associates:

- A message offset
- A byte offset within the segment's `.log` file

The byte offset points to the beginning of the corresponding stored record in the `.log` file. That means it should point to the record's 8-byte `Offset` field, not to the middle of the record body.

## Entry Layout

For now, each `.index` entry should store:

1. Message offset: 8 bytes
2. Byte offset in `.log`: 8 bytes

This simple fixed-width layout is easy to append, scan, and rebuild during recovery.

## Lookup Semantics

When the broker receives a fetch request for a message offset:

1. It locates the segment whose base offset is the greatest base offset less than or equal to the requested offset
2. It consults that segment's `.index` file
3. It finds the byte offset for the requested message offset
4. It seeks to that byte position in the `.log` file
5. It reads and decodes the record from the `.log` file

This lets the broker jump directly into the `.log` file instead of sequentially scanning the whole segment.

## Write Semantics

While a segment is active, the broker appends new `.index` entries as new messages are appended to the `.log` file.

Append behavior follows these rules:

- Only the active segment's `.index` file may receive new entries
- New entries are appended in ascending offset order
- Existing entries are not rewritten during normal append flow
- Writing to an inactive segment's `.index` file is not allowed

Each appended `.index` entry must correspond to a committed record in the active `.log` file.

## Relationship to `.log`

The `.index` file depends on the `.log` file's framing and byte layout.

For each appended record:

1. The broker determines the starting byte position of the record in the `.log` file
2. The broker appends the record to `.log`
3. The broker appends an `.index` entry mapping the record offset to that byte position

Because `.index` entries point into `.log`, the `.log` file must keep persisted record boundaries stable for as long as the segment exists.

## Relationship to `.timeindex`

The `.timeindex` file maps timestamps to message offsets. The `.index` file completes timestamp-based lookup by translating that offset into a byte position in the `.log` file.

For timestamp-based fetch:

1. `.timeindex` resolves the requested timestamp to a message offset
2. `.index` resolves that message offset to a byte position
3. `.log` provides the record bytes at that position

## Active and Inactive State

The active state of the segment is determined by whether the segment has an active file handle.

For the `.index` file this means:

- If the segment is active, the `.index` file may receive appended entries
- If the segment is inactive, the `.index` file becomes read-only from the broker's perspective

There should be exactly one active `.index` file per partition at a time because there is exactly one active segment per partition.

## Rollover Behavior

The `.index` file does not roll independently.

When the segment rolls because `segment.bytes` or `segment.ms` is exceeded:

1. The current segment becomes inactive
2. A new segment is created
3. A new `.log`, `.index`, and `.timeindex` file are created together with the same new base filename
4. Future entries are appended only to the new segment's `.index` file

## Recovery Behavior

On broker restart:

- The broker scans the partition directory and discovers segment files
- The base offset of each `.index` file is reconstructed from its filename
- The segment with the highest base offset is selected as the recovered active segment
- The broker assigns the active file handle to that segment

Because the `.index` file is derived from `.log`, recovery may rebuild or validate it from the `.log` file if needed.

### Rebuild Strategy

For now, the simplest recovery strategy is:

1. Scan the segment's `.log` file from the beginning
2. For each complete record, capture the record offset and its starting byte position
3. Recreate `.index` entries from those observed values
4. Truncate or rewrite the `.index` file if it contains entries that point past the valid end of `.log`

This keeps the `.index` file recoverable even if the broker crashes after writing `.log` but before fully updating `.index`.

If the broker appended the record to `.log` but only partially appended its `.index` entry, recovery should reuse the same scan-from-beginning process used for truncated `.log` handling. When a partial or stale `.index` entry is found, the broker should rewrite the recovered entry so it points to the correct byte offset in `.log`.

If segment rollover completed for `.log` but the broker crashed before the new `.index` file was created or fully initialized, recovery should:

1. Check the latest `.log` file for its matching `.index`
2. If the file is missing or invalid, locate the last valid `.index` file before it
3. Rebuild `.index` files from that point forward through the current active segment

This allows recovery to handle both interrupted append and interrupted rollover using the `.log` file as the source of truth.

## Deletion Behavior

Inactive `.index` files are deleted only as part of whole-segment deletion.

The `.index` file should not be removed independently from its `.log` and `.timeindex` companions. A segment is deleted as a unit so the three files stay consistent with one another.

## Invariants

The implementation should preserve the following invariants for `.index` files:

- Each `.index` file belongs to exactly one segment
- Each `.index` file shares its base filename with one `.log` and one `.timeindex`
- The filename is the segment base offset encoded as a 20-digit zero-padded decimal string
- The first `.index` file in a partition is `00000000000000000000.index`
- Only the active `.index` file may accept appended entries
- Entries are appended in ascending message offset order
- Each `.index` entry points to the start of a stored record in `.log`
- Inactive `.index` files may only be removed through whole-segment deletion

## Testing Focus

Tests for the `.index` file should cover at least:

- Creating the first `.index` file as `00000000000000000000.index`
- Storing `.index` files in the partition directory
- Using the same base filename for `.index`, `.log`, and `.timeindex`
- Appending entries only to the active `.index` file
- Refusing writes to inactive `.index` files
- Mapping offsets to the correct starting byte positions in `.log`
- Looking up records by offset through `.index`
- Creating a new `.index` file when the segment rolls
- Rebuilding `.index` from `.log` during recovery
- Ignoring or truncating stale `.index` entries that point beyond the valid end of `.log`
- Rewriting partially appended `.index` entries to the correct byte offsets during recovery
- Rebuilding missing `.index` files after a crash that interrupted segment rollover

## Deferred Topics

The `.index` file design still leaves these choices for later:

- Whether index entries should be sparse or dense
- Whether relative offsets should be used instead of full 64-bit offsets
- Whether index rebuild should always rewrite the file or only validate and truncate when needed
