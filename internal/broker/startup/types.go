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
	// Image *Image
}

type FetchMetadataImageRequest struct {
	ClusterID string
	NodeID    int32
}

type FetchMetadataImageResponse struct {
	Error Error
	// Image *Image
}