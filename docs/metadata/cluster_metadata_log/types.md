# Cluster Metadata Log Types

## Purpose

This document defines the durable record types stored in the
`__cluster_metadata` log. The log-level behavior, ownership, framing, replay,
and recovery rules are defined in
[cluster_metadata_log.md](./cluster_metadata_log.md).

## Record Envelope

Each metadata record body is wrapped in a small envelope:

| Field | Type | Required | Description |
| --- | --- | --- | --- |
| `Type` | uint8 | Yes | Metadata record type ID |
| `DataLength` | int32 | Yes | Length of the serialized type-specific payload |
| `Data` | bytes | Yes | Type-specific payload |

Unknown record types must be rejected during append. During recovery, an
unknown type should stop replay and return an error because the broker cannot
safely interpret the metadata state that follows.

## Record Types

The first version supports these concrete metadata record types:

| Type ID | Type | Meaning |
| --- | --- | --- |
| `1` | `TopicCreationRecord` | Creates a topic metadata entry |
| `2` | `PartitionCreationRecord` | Creates a partition metadata entry |
| `3` | `PartitionUpdateRecord` | Updates partition assignment, leader, or ISR metadata |
| `4` | `BrokerRegistrationRecord` | Creates broker metadata |

`PartitionRecord` is a shared base model for partition metadata records. It is
not written directly as its own record type.

## Topic Identity

Topic name is the unique key for topic metadata.

The metadata layer does not need a separate topic ID at this stage because topic
names are required to be unique. Any metadata record that refers to a topic must
use the topic name as the topic identifier.

Validation rules:

- Topic names must be non-empty
- Topic names must pass the topic name validation rules
- A `TopicCreationRecord` must not create a topic name that already exists in
  the replayed metadata image

## TopicCreationRecord

A `TopicCreationRecord` represents topic creation.

Payload fields:

| Field | Type | Required | Description |
| --- | --- | --- | --- |
| `Name` | string | Yes | Unique topic name |
| `Config` | `metadata.TopicConfig` | Yes | Minimal topic-level configuration values used until the topic package owns a concrete config type |
| `CreatedAt` | timestamp | Yes | Time the topic metadata entry was created |

Validation rules:

- `Name` must be non-empty
- `Name` must be unique
- `Config` must pass the metadata topic config validation rules
- `CreatedAt` must be set

Recommended `Data` layout:

1. Name length: 4 bytes
2. Name bytes: variable length
3. Config.Partitions: 4 bytes
4. Config.ReplicationFactor: 2 bytes
5. Config.RetentionMs presence flag: 1 byte
6. Config.RetentionMs value when present: 8 bytes
7. Config.RetentionBytes presence flag: 1 byte
8. Config.RetentionBytes value when present: 8 bytes
9. Config.MinInSyncReplicas: 2 bytes
10. CreatedAt: 8 bytes

`CreatedAt` should be encoded as Unix time in milliseconds since epoch.

## Base PartitionRecord

`PartitionRecord` is the shared base payload for concrete partition metadata
records.

Base fields:

| Field | Type | Required | Description |
| --- | --- | --- | --- |
| `TopicName` | string | Yes | Unique topic name this partition belongs to |
| `PartitionId` | int32 | Yes | Partition number within the topic |
| `LeaderBrokerId` | int32 | Yes | Broker currently responsible for leader duties |
| `ReplicaBrokerIds` | array of int32 | Yes | Brokers assigned as replicas for the partition |

The base partition record intentionally does not include ISR broker IDs. ISR is
only present on `PartitionUpdateRecord`.

The partition metadata key is `(TopicName, PartitionId)`.

Base validation rules:

- `TopicName` must be non-empty
- `TopicName` must refer to an existing topic in the replayed metadata image
- `PartitionId` must be greater than or equal to zero
- `PartitionId` must be less than the topic's `Config.Partitions`
- `LeaderBrokerId` must appear in `ReplicaBrokerIds`
- `ReplicaBrokerIds` must not be empty
- `ReplicaBrokerIds` must not contain duplicates

Recommended base `Data` layout:

1. TopicName length: 4 bytes
2. TopicName bytes: variable length
3. PartitionId: 4 bytes
4. LeaderBrokerId: 4 bytes
5. Replica count: 4 bytes
6. Repeated replica broker IDs: 4 bytes each

Concrete partition records extend this base layout by appending their own
fields.

## PartitionCreationRecord

A `PartitionCreationRecord` creates metadata for one partition.

It extends the base `PartitionRecord` with:

| Field | Type | Required | Description |
| --- | --- | --- | --- |
| `CreatedAt` | timestamp | Yes | Time the partition metadata entry was created |

Validation rules:

- All base `PartitionRecord` validation rules must pass
- The `(TopicName, PartitionId)` key must not already exist
- `CreatedAt` must be set

Recommended `Data` layout:

1. Base `PartitionRecord` data
2. CreatedAt: 8 bytes

`CreatedAt` should be encoded as Unix time in milliseconds since epoch.

## PartitionUpdateRecord

A `PartitionUpdateRecord` updates metadata for an existing partition. It can
represent assignment changes, leader changes, and ISR updates.

It extends the base `PartitionRecord` with:

| Field | Type | Required | Description |
| --- | --- | --- | --- |
| `ISRBrokerIds` | array of int32 | Yes | Brokers currently in the in-sync replica set |
| `UpdatedAt` | timestamp | Yes | Time this partition metadata entry was updated |

Validation rules:

- All base `PartitionRecord` validation rules must pass
- The `(TopicName, PartitionId)` key must already exist
- `ISRBrokerIds` must be a subset of `ReplicaBrokerIds`
- `ISRBrokerIds` must not contain duplicates
- `UpdatedAt` must be set

Recommended `Data` layout:

1. Base `PartitionRecord` data
2. ISR count: 4 bytes
3. Repeated ISR broker IDs: 4 bytes each
4. UpdatedAt: 8 bytes

`UpdatedAt` should be encoded as Unix time in milliseconds since epoch.

## BrokerRegistrationRecord

A `BrokerRegistrationRecord` creates broker metadata.

This record is appended by the leader controller when a broker starts and calls
the controller quorum startup API defined in [api.md](../../quorum/api.md).

This first phase does not support broker refresh, heartbeat, or fencing records.
A broker ID can be registered only once in the metadata log. Broker restart
idempotency is handled by the leader controller before append: if the current
metadata image already contains matching broker metadata for the same broker ID,
the controller returns the current image without appending a duplicate
`BrokerRegistrationRecord`.

Payload fields:

| Field | Type | Required | Description |
| --- | --- | --- | --- |
| `ClusterID` | string | Yes | Cluster identifier the broker is joining |
| `BrokerID` | int32 | Yes | Unique broker node ID |
| `Host` | string | Yes | Broker host used for client, broker, and controller traffic in this phase |
| `Port` | int32 | Yes | Broker port used for client, broker, and controller traffic in this phase |
| `LogDir` | string | No | Broker-local log directory from node configuration |
| `RegisteredAt` | timestamp | Yes | Time this registration was accepted by the leader controller |

Validation rules:

- `ClusterID` must be non-empty and must match the replayed metadata image's
  cluster ID
- `BrokerID` must be greater than or equal to zero
- `Host` must be non-empty
- `Port` must be greater than zero
- `RegisteredAt` must be set
- If the broker ID does not exist in the image, the record creates it
- If the broker ID already exists in the image, replaying another
  `BrokerRegistrationRecord` is invalid even when the endpoint metadata is
  identical

Recommended `Data` layout:

1. ClusterID length: 4 bytes
2. ClusterID bytes: variable length
3. BrokerID: 4 bytes
4. Host length: 4 bytes
5. Host bytes: variable length
6. Port: 4 bytes
7. LogDir length: 4 bytes
8. LogDir bytes: variable length
9. RegisteredAt: 8 bytes

`RegisteredAt` should be encoded as Unix time in milliseconds since epoch.

Application rules:

- A successful registration creates a `BrokerMetadata` entry keyed by
  `BrokerID`
- `RegisteredAt` and `UpdatedAt` should both use the record's `RegisteredAt`
  timestamp
- Broker restart with identical metadata does not create a metadata-log record;
  heartbeat, shutdown, fencing, and broker metadata update behavior should be
  added as separate metadata-log record types later
- Partition records should reference brokers by broker ID. When broker metadata
  is available, partition leader, replica, and ISR broker IDs should refer to
  registered brokers
