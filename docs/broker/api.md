# Broker APIs

## Public Interface

The broker package should expose only the minimal startup,
configuration-loading, and leader-partition lookup APIs required by this phase.

Recommended initial surface:

```go
type Loader interface {
	Load(path string) (NodeConfig, error)
}

type ServerStartCommand interface {
	Start(args []string) error
}

type StartupDiscovery interface {
	RegisterAndFetchMetadata(config *NodeConfig) (metadata.Image, error)
}

type LeaderPartitions interface {
	CurrentLeaderPartitions() []metadata.PartitionMetadata
	IsLeaderFor(topicName string, partitionID int32) bool
}
```

`RegisterAndFetchMetadata` is the startup API that handles controller quorum
discovery:

- it chooses a random controller endpoint from `config.ControllerQuorum`
- it sends `RegisterBrokerAndFetchMetadata` over the binary TCP protocol using
  `config.ClusterID`, `config.NodeID`, and broker contact metadata
- if the controller replies with `NOT_LEADER`, a leader node ID, and a leader
  address, it stores that endpoint in `config.LeaderController`
- it retries the same request against `config.LeaderController`
- if the controller replies with `NOT_LEADER` without a leader endpoint,
  `LEADER_UNKNOWN`, `INTERNAL_ERROR`, or the endpoint cannot be reached, it
  tries another controller from the remaining quorum endpoints until the startup
  timeout is reached
- if a fetched metadata image has a `ClusterID` that differs from
  `config.ClusterID`, startup fails
- it returns the fetched metadata image from the leader or returns an error if
  discovery fails

`ServerStartCommand` represents the `kafka-server-start.sh` entry point. It
parses CLI arguments, requires exactly one `config.path=<path>` argument,
rejects unknown CLI arguments, loads the properties file from `config.path`,
rejects combined roles, and starts the broker or controller startup path based
on the loaded role. Invalid CLI usage aborts before reading any config file.

Startup communication uses a `10s` default timeout for TCP connect, request
frame write, response frame read, and each full startup API call. Tests may
inject shorter timeout values, but production defaults should be bounded and
must not retry forever.

The first startup implementation should include the real TCP client and server
path for these startup APIs. In-process interfaces may still be used beneath the
transport for unit tests and handler composition, but they are not a substitute
for broker-controller communication in the end-to-end startup flow.

`LeaderPartitions` exposes the broker-local derived leadership view:

- `CurrentLeaderPartitions` returns copies of partitions currently led by this
  broker
- `IsLeaderFor` returns whether the current leader partition state contains the
  requested partition key

Consumer-group APIs are specified in
[api.md](../group_coordinator/api.md), because they
are owned by the broker that leads the assigned `__consumer_offsets` partition.

Produce, replication, metadata propagation, controller communication, and full
fetch semantics should be specified later in separate broker or protocol design
work.
