package broker

import (
	"github.com/thuhaung/kafka/internal/broker/config"
)

type ErrorCode int16

const (
	ErrorNone                      ErrorCode = 0
	ErrorNotLeader                 ErrorCode = 1
	ErrorLeaderUnknown             ErrorCode = 2
	ErrorClusterMismatch           ErrorCode = 3
	ErrorInvalidRequest            ErrorCode = 4
	ErrorInvalidBrokerRegistration ErrorCode = 5
	ErrorInternalError             ErrorCode = 6
	ErrorTopicAlreadyExists        ErrorCode = 7
)

type Error struct {
	Code             ErrorCode
	Message          string
	LeaderController *config.ControllerEndpoint
}

type RegisterBrokerAndFetchMetadataRequest struct {
	ClusterID      string
	BrokerID       int32
	Host           string
	Port           int32
	ControllerHost string
	ControllerPort int32
	LogDir         string
}

type RegisterBrokerAndFetchMetadataResponse struct {
	Error Error
	Image *Image
}

type FetchMetadataImageRequest struct {
	ClusterID string
	NodeID    int32
}

type FetchMetadataImageResponse struct {
	Error Error
	Image *Image
}

// // PartitionKey identifies one partition inside the metadata image.
// type PartitionKey struct {
// 	TopicName   string
// 	PartitionID int32
// }

type Image struct {
	ClusterID     string
	AppliedOffset int64
	// Topics        map[string]TopicMetadata
	// Partitions    map[PartitionKey]PartitionMetadata
	// Brokers       map[int32]BrokerMetadata
}

// // TopicConfig stores the minimal topic configuration needed by startup callers.
// type TopicConfig struct {
// 	Partitions        int32
// 	ReplicationFactor int16
// 	RetentionMs       int64
// 	CompactionEnabled bool
// }

// // TopicMetadata describes one topic in the cluster metadata image.
// type TopicMetadata struct {
// 	Name      string
// 	Config    TopicConfig
// 	CreatedAt time.Time
// }

// // PartitionMetadata describes the placement and leadership of one partition.
// type PartitionMetadata struct {
// 	TopicName        string
// 	PartitionID      int32
// 	LeaderBrokerID   int32
// 	ReplicaBrokerIDs []int32
// 	ISRBrokerIDs     []int32
// 	CreatedAt        time.Time
// 	UpdatedAt        time.Time
// }

// // BrokerMetadata describes how to contact a broker in the cluster image.
// type BrokerMetadata struct {
// 	BrokerID       int32
// 	Host           string
// 	Port           int32
// 	ControllerHost string
// 	ControllerPort int32
// 	LogDir         string
// 	RegisteredAt   time.Time
// 	UpdatedAt      time.Time
// }
