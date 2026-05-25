This fixture provides a starter `__cluster_metadata` partition for developing
and testing metadata-image replay.

Layout:

- `__cluster_metadata-0/0.log`
- `__cluster_metadata-0/0.index`
- `__cluster_metadata-0/0.timeindex`
- `manifest.json`

Important notes:

- The durable records in the `.log` file are metadata records, not `Image`
  values. `Image` is the in-memory state rebuilt by replaying those records.
- The record bodies use the envelope described in
  `docs/metadata/cluster_metadata_log/types.md`:
  `Type(uint8) + DataLength(int32) + Data(bytes)`.
- The `Type` byte uses this mapping:
  `TopicCreation=1`, `PartitionCreation=2`, `PartitionUpdate=3`,
  `BrokerRegistration=4`.
- The outer segment framing follows the current implementation in
  `internal/storage/segment`:
  `Offset(int64) + Length(int32) + Record(bytes)`.
- The filenames intentionally match the current code, which writes `0.log`,
  `0.index`, and `0.timeindex`.

The fixture currently contains three records:

1. `BrokerRegistrationRecord` for broker `1`
2. `TopicCreationRecord` for topic `orders`
3. `PartitionCreationRecord` for `orders-0`

`manifest.json` mirrors those records in readable form so you can verify your
decoder and image builder while keeping the actual segment files binary.
