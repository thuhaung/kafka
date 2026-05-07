# Partition APIs

## Purpose

This document defines the partition-facing storage layout and the lifecycle
expectations that an implementation should expose around partition directories.

## Partition Log Directory Layout

Each partition owns a log directory named after `PartitionName`.

Inside that directory, the partition stores one segment file trio per segment:

- one `.log` file
- one `.index` file
- one `.timeindex` file

This means every segment contributes exactly three files to the partition log
directory.

Examples for partition `orders-0`:

- `orders-0/00000000000000000000.log`
- `orders-0/00000000000000000000.index`
- `orders-0/00000000000000000000.timeindex`

After a segment roll, if the next segment starts at offset `42`, the partition
directory also contains:

- `orders-0/00000000000000000042.log`
- `orders-0/00000000000000000042.index`
- `orders-0/00000000000000000042.timeindex`

As the segment rolls over according to [segment.md](../log/segment.md), new
segment file trios are added to the partition log directory. Older segments
remain in the directory as inactive segments until retention deletes them.

## Relationship to Segment Rollover

Partition segment creation follows the rollover rules defined in
[segment.md](../log/segment.md).

When a rollover happens:

1. The current active segment becomes inactive.
2. A new segment base offset is chosen.
3. One new `.log` file is created.
4. One new `.index` file is created.
5. One new `.timeindex` file is created.
6. The new file trio is added to the partition log directory.

There is still only one active segment per partition, but the log directory
accumulates multiple inactive segment file trios over time.

## Recommended Interfaces

The exact API surface may evolve, but the partition package should expose
helpers close to:

```go
func NewMetadata(topicName string, partitionID int32, leader int, replicas []int, isr []int) (Metadata, error)
func PartitionName(topicName string, partitionID int32) string
func Validate(metadata Metadata) error
```

`NewMetadata` is a package-level constructor helper, not a network API or CLI
API. It exists so callers do not manually assemble invalid partition metadata.
The helper should derive `PartitionName` from `topicName` and `partitionID`,
populate the metadata fields, and run the same validation rules as
`Validate` before returning.

Directory management may live in `internal/storage/log` if it is more naturally
owned by the log implementation, but partition-level callers should still see a
single partition directory containing segment file trios.

## Example Directory

Example partition log directory after one rollover:

```text
orders-0/
  00000000000000000000.log
  00000000000000000000.index
  00000000000000000000.timeindex
  00000000000000000042.log
  00000000000000000042.index
  00000000000000000042.timeindex
```
