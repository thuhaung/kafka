# Metadata Directory

## Purpose

This directory contains design notes for broker metadata and internal metadata
logs.

Each metadata component should live in its own subdirectory. The component's
main document explains ownership, behavior, replay, recovery, and testing. A
neighboring `types.md` document captures the durable record types or recommended
in-memory types for that component.

## Directory Guide

- [cluster_metadata.md](./cluster_metadata/cluster_metadata.md): in-memory
  cluster metadata image built by replaying cluster metadata records
- [types.md](./cluster_metadata/types.md): recommended Go types for the cluster
  metadata image
- [cluster_metadata_log.md](./cluster_metadata_log/cluster_metadata_log.md):
  append-only log for topic, partition, and broker metadata changes
- [types.md](./cluster_metadata_log/types.md): durable record types stored in
  the `__cluster_metadata` log
- [consumer_offsets_log.md](./consumer_offsets_log/consumer_offsets_log.md):
  append-only log for consumer group membership and committed offsets
- [types.md](./consumer_offsets_log/types.md): durable record types stored in
  the `__consumer_offsets` log
