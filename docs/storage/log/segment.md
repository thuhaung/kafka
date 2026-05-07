# Segment Design

## Purpose

The segment component defines how a partition log is split into manageable files on disk.

Each segment groups the storage files and metadata needed to append, read, and eventually delete a bounded slice of a partition's records. In this project, a segment is the unit used for log growth, offset-based lookup support, timestamp-based lookup support, and retention-oriented deletion.

## Goals

The segment design must provide:

- A predictable on-disk layout for partition log data
- A clear relationship between raw record storage and lookup indexes
- Safe append behavior for the active segment only
- Segment rollover based on size and time
- A deletion model for inactive segments

## Non-Goals

The initial segment design does not attempt to support:

- Rolling index files independently from their corresponding `.log` file
- Partial deletion within a segment
- Segment compaction mechanics in this document
- Recovery of corrupt files beyond establishing the expected file layout and ownership rules

Log compaction should be ignored for now. Segment deletion is driven only by rollover and retention rules, not by key-based compaction or tombstone cleanup.

Those concerns can be expanded later in the log and cleaner designs.

## Segment Model

Each partition stores its segments inside a directory named after the partition.

Within that partition directory, each segment contains a file trio:

- `.log`
- `.index`
- `.timeindex`

Each segment also has in-memory metadata maintained by the broker, including:

- Base offset
- Creation time
- Active state

The base offset identifies the first message stored in the segment and is used as the segment filename prefix.

## Configuration

The segment and partition lifecycle is controlled by a small set of configuration values.

| Config | Scope | Meaning | Default |
| --- | --- | --- | --- |
| `segment.bytes` | Segment | Maximum size of the active segment `.log` file before the next append rolls to a new segment | `1 GiB` |
| `segment.ms` | Segment | Maximum age of the active segment before the next append rolls to a new segment | `7 days` |
| `retention.ms` | Message retention policy | Delete inactive segments whose messages are older than the configured age | `7 days` |
| `retention.bytes` | Partition retention policy | Delete oldest inactive segments when the total size of all segments in a partition exceeds the configured limit | `-1` meaning unlimited |

These defaults are intentionally conservative and optimized for a learning-oriented implementation rather than Kafka-level tuning flexibility.

## On-Disk Layout

For a partition directory such as `topic-0`, a segment whose first record offset is `0` would be stored as:

- `topic-0/00000000000000000000.log`
- `topic-0/00000000000000000000.index`
- `topic-0/00000000000000000000.timeindex`

If the next segment starts at offset `42`, its files would be:

- `topic-0/00000000000000000042.log`
- `topic-0/00000000000000000042.index`
- `topic-0/00000000000000000042.timeindex`

By default, the first segment in a partition must begin at offset `0`.

### Filename Format

Segment filenames are derived from the 64-bit base offset and encoded as zero-padded decimal strings.

The base filename must always be 20 digits wide so lexical ordering matches numeric offset ordering.

Examples:

- Offset `0` becomes `00000000000000000000`
- Offset `42` becomes `00000000000000000042`
- Offset `123456` becomes `00000000000000123456`

## Segment State

A segment has one of two logical states:

### Active

An active segment is the only segment in the partition that can accept appended messages.

While active:

- New messages may be appended to the `.log` file
- Related `.index` and `.timeindex` entries may be written for those appended messages
- No other write-oriented operation should modify segment contents
- The segment must not be deleted

There should be at most one active segment per partition at a time.

### Inactive

An inactive segment no longer accepts appended messages.

While inactive:

- Messages cannot be appended
- The segment becomes eligible for whole-segment deletion
- Reads through the `.log`, `.index`, and `.timeindex` files remain valid until deletion

This model keeps append behavior simple and makes retention-oriented deletion easier because the broker removes whole inactive segments instead of mutating old ones in place.

## The `.log` File

The `.log` file stores the raw binary data for each message in the segment.

### Responsibilities

The `.log` file is responsible for:

- Persisting serialized message bytes in append order
- Preserving message ordering within the segment
- Serving as the source of truth for record bytes during fetch

### Naming

The filename of a `.log` file is the offset of the first message stored in that file.

Examples:

- `0.log` means the first message in the segment has offset `0`
- `1000.log` means the first message in the segment has offset `1000`

### Size-Based Rollover

Each topic configures a maximum segment size through `segment.bytes`.
Each topic may also configure a maximum segment age through `segment.ms`.

Before appending a new message, the broker must determine whether either rollover condition has been exceeded:

- Writing the new message would cause the active `.log` file to exceed `segment.bytes`
- The active segment has existed long enough to exceed `segment.ms`

If either condition is exceeded:

1. The current active segment becomes inactive
2. A new `.log`, `.index`, and `.timeindex` file are created with the same base filename
3. The new segment's base offset is the offset of the message about to be appended
4. The message is appended to the new segment instead

This means the message that would overflow the current segment is never partially written to the old segment.

### Time-Based Rollover

`segment.ms` defines how long the active segment may remain open before it must be rolled on the next append attempt.

Time-based rollover does not happen through a background thread in this design.

Instead, rollover is append-driven. When the segment receives new data, the broker checks both `segment.bytes` and `segment.ms`. If either limit has already been exceeded, the broker rolls the segment before appending the new message.

When time-based rollover is triggered on append:

1. The current active segment becomes inactive
2. A new `.log`, `.index`, and `.timeindex` file are created with the same base filename prefix
3. The new message is written to the new active segment

Time-based rollover is still driven by the partition leader broker because that broker controls append ordering for the partition.

## The `.index` File

The `.index` file maps message offsets to byte offsets within the `.log` file.

This mapping allows the broker to locate where a requested message begins in the segment's raw log bytes without scanning the entire file from the start.

### Responsibilities

The `.index` file is used to:

- Support fetch by logical message offset
- Narrow the read position in the `.log` file
- Speed up random access compared with full linear scans

### Lookup Role

Given a requested message offset, the broker uses the `.index` file to find the corresponding byte position in the `.log` file, then reads and decodes records from that position.

### Current Scope

Index rollover mechanics are explicitly out of scope for now.

For the current design, the `.index` file is treated as part of the segment file trio and rolls together with the segment rather than on an independent policy.

## The `.timeindex` File

The `.timeindex` file maps timestamps to message offsets.

This file is used for timestamp-based lookup rather than direct byte lookup.

### Responsibilities

The `.timeindex` file is used to:

- Support fetch by timestamp
- Identify the message offset associated with a requested time
- Work together with the `.index` file for efficient reads

### Lookup Role

For fetch by timestamp:

1. The broker consults the `.timeindex` file to find the offset associated with the requested timestamp
2. The broker consults the `.index` file to translate that message offset into a byte position in the `.log` file
3. The broker reads from the `.log` file and returns the matching records

This keeps timestamp lookup as a two-step process: timestamp to offset, then offset to byte location.

### Current Scope

Rolling `.timeindex` independently is also out of scope for now.

Like `.index`, the `.timeindex` file is treated as a fixed companion to the segment's `.log` file.

## Segment Lifecycle

The expected lifecycle of a segment is:

1. Create a new active segment with `.log`, `.index`, and `.timeindex` files sharing the same base filename
2. Append records only to the active segment
3. Update index structures as records are appended
4. On each append, check whether `segment.bytes` or `segment.ms` has been exceeded
5. Roll the segment when either policy requires it
6. Mark the old segment inactive
7. Retain inactive segments for reads until retention or cleanup removes them
8. Delete the entire inactive segment as a unit when eligible

## Retention

Retention is enforced at the inactive-segment level. The broker never deletes individual messages from the middle of a segment.

### `retention.ms`

`retention.ms` is better thought of as a message-retention policy than a segment-shape policy.

It means the leader broker's log cleaner thread should delete inactive segments whose messages are older than the configured value.

For this design:

- `retention.ms` is evaluated only for inactive segments
- The active segment is never deleted by retention while it is still the active append target
- Segment deletion is whole-file deletion of the `.log`, `.index`, and `.timeindex` trio

### `retention.bytes`

`retention.bytes` is better thought of as a partition-retention policy than a segment-local policy.

It means the leader broker's log cleaner thread should check whether the total size of all segments for a partition exceeds the configured limit. If it does, the broker deletes oldest inactive segments until the partition fits within the configured budget.

For this design:

- The total includes all segment files that belong to the partition
- Deletion proceeds from the oldest inactive segment forward
- The active segment should be preserved unless a later design explicitly allows a stricter policy

### Log Compaction

Log compaction is out of scope for now.

The cleaner should only apply retention-based deletion using `retention.ms` and `retention.bytes`. It should not attempt key-based compaction, tombstone retention, or record rewriting inside existing segments.

## Invariants

The implementation should preserve the following invariants:

- Every segment belongs to exactly one partition directory
- Every segment has exactly one `.log`, one `.index`, and one `.timeindex`
- The three files in a segment share the same base filename
- Segment filenames are derived from the 64-bit segment base offset
- Segment filenames use a 20-digit zero-padded decimal encoding
- The first segment in a partition begins at offset `0`
- Only one segment per partition may be active at a time
- Only the active segment accepts appends
- Inactive segments may be deleted as a whole
- Offset lookup uses `.index`
- Timestamp lookup uses `.timeindex` together with `.index`

## Broker Responsibilities

The partition leader broker is responsible for:

- Creating the first segment for a partition
- Assigning message offsets
- Appending records to the active segment
- Enforcing `segment.bytes`
- Enforcing `segment.ms` during append handling
- Creating new segments on rollover
- Marking previous segments inactive
- Serving fetches using `.index` and `.timeindex`

## Failure and Recovery Considerations

At minimum, recovery logic should be designed around these expectations:

- On broker restart, the broker should discover segments by scanning the partition directory
- Segment metadata such as base offset can be reconstructed from filenames
- Active state is determined by whether the segment has an active file handle
- On broker restart, previously open file handles are gone, so the broker must recover active state explicitly
- The segment with the highest base offset should be selected as the active segment after recovery
- Recovery should assign an active file handle to that highest-base-offset segment and treat older segments as inactive
- If the latest `.log` file exists but its `.index` or `.timeindex` file is missing because rollover was interrupted by a crash, recovery should locate the last valid index file before it and rebuild forward until the active segment is consistent again
- If `.index` or `.timeindex` contains a partial trailing entry or stale value after a crash, recovery should rescan from the beginning and rewrite the recovered entries so each value points to the correct byte position or offset in the corresponding `.log`

Detailed crash recovery rules can be expanded in the broader log design.

## Testing Focus

Segment-related tests should cover at least:

- Creating the first segment at offset `0`
- Encoding segment filenames as zero-padded 20-digit decimal offsets
- Storing segments inside the partition-named directory
- Creating all three files for a segment with the same base filename
- Appending only to the active segment
- Refusing appends to inactive segments
- Rolling to a new segment when `segment.bytes` would be exceeded
- Rolling to a new segment on append when `segment.ms` has been exceeded
- Creating new `.log`, `.index`, and `.timeindex` files together on rollover
- Naming new segment files from the first offset written into them
- Applying `retention.ms` only to inactive segments
- Applying `retention.bytes` across the full partition and deleting oldest inactive segments first
- Looking up records by offset through `.index`
- Looking up records by timestamp through `.timeindex` and `.index`
- Reconstructing the active segment on broker recovery by choosing the highest base offset
- Rebuilding missing or stale index files from the last valid segment through the recovered active segment
- Deleting inactive segments as whole units

## Open Questions

The current design intentionally leaves several choices open for later documents and implementation:

- Whether index entries should be sparse or dense
- How duplicate timestamps should be handled in a later version
- What retention edge cases should do if the active segment alone exceeds `retention.bytes`
