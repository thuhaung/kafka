# Cluster Metadata Types

## Purpose

This document defines the recommended in-memory types used by the cluster
metadata image. Durable `__cluster_metadata` records are defined in
[types.md](../cluster_metadata_log/types.md).

## Proposed Go Types

The exact package path may evolve, but `internal/metadata` should expose types
close to:

```go
package metadata

import "time"

type PartitionKey struct {
	TopicName   string
	PartitionID int32
}

type Image struct {
	ClusterID     string
	AppliedOffset int64
	Topics        map[string]TopicMetadata
	Partitions    map[PartitionKey]PartitionMetadata
	Brokers       map[int32]BrokerMetadata
}

type TopicConfig struct {
	Partitions        int32
	ReplicationFactor int16
	RetentionMs       int64
	CompactionEnabled bool
}

type TopicMetadata struct {
	Name      string
	Config    TopicConfig
	CreatedAt time.Time
}

type PartitionMetadata struct {
	TopicName        string
	PartitionID      int32
	LeaderBrokerID   int32
	ReplicaBrokerIDs []int32
	ISRBrokerIDs     []int32
	CreatedAt        time.Time
	UpdatedAt        time.Time
}

type BrokerMetadata struct {
	BrokerID       int32
	Host           string
	Port           int32
	ControllerHost string
	ControllerPort int32
	LogDir         string
	RegisteredAt   time.Time
	UpdatedAt      time.Time
}
```

Implementation notes:

- `TopicConfig` is intentionally minimal until the topic package owns a concrete
  configuration type. When `internal/topic` is implemented, the metadata image
  should either reuse that type directly or keep this type aligned with it.
- Slices and maps returned by public APIs should be copied before returning so
  callers cannot mutate the image
- `AppliedOffset` is the highest cluster metadata log offset included in this
  image. It starts at `-1` for an empty image and advances only after a metadata
  record is successfully validated and applied. Consumers can use it to tell how
  fresh an image is relative to the log.
- If `ControllerHost` or `ControllerPort` is unset, callers may use `Host` and
  `Port` for controller-facing communication only when the broker/controller
  design allows that listener fallback
