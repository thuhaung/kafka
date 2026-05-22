package broker

import (
	"github.com/thuhaung/kafka/internal/broker/config"
	"github.com/thuhaung/kafka/internal/network"
)

// PartitionKey identifies one partition within the cluster.
type PartitionKey struct {
	TopicName   string
	PartitionID int32
}

// PartitionMetadata is the broker-local partition leadership metadata needed
// before produce, fetch, and replication APIs are introduced.
type PartitionMetadata struct {
	TopicName        string
	PartitionID      int32
	LeaderBrokerID   int
	ReplicaBrokerIDs []int
	ISRBrokerIDs     []int
}

// LeaderPartitionState is a derived, broker-local view of partitions currently
// led by BrokerID.
type LeaderPartitionState struct {
	BrokerID   int
	Partitions map[PartitionKey]PartitionMetadata
}

type Broker struct {
	nodeConfig *config.NodeConfig
	servers []*network.Server
}
