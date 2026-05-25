package broker

import (
	"sort"
	"time"

	"github.com/thuhaung/kafka/internal/metadata"
	"github.com/thuhaung/kafka/internal/protocol"
)

type ErrorCode int16

const (
	ErrorNone ErrorCode = iota
	ErrorNotLeader
	ErrorLeaderUnknown
	ErrorClusterMismatch
	ErrorInvalidRequest
	ErrorInvalidBrokerRegistration
	ErrorInternal
	ErrorTopicAlreadyExists
)

type ControllerEndpoint struct {
	NodeID int32
	Host   string
	Port   int32
}

type Error struct {
	Code             ErrorCode
	Message          string
	LeaderController *ControllerEndpoint
}

type RegisterBrokerAndFetchMetadataRequest struct {
	ClusterID string
	BrokerID  int32
	Host      string
	Port      int32
	LogDir    string
}

type RegisterBrokerAndFetchMetadataResponse struct {
	Error Error
	Image *metadata.Image
}

type FetchMetadataImageRequest struct {
	ClusterID string
	NodeID    int32
}

type FetchMetadataImageResponse struct {
	Error Error
	Image *metadata.Image
}

func (request *RegisterBrokerAndFetchMetadataRequest) Encode() ([]byte, error) {
	encoder := protocol.NewEncoder()
	encoder.WriteStringWithUint16Length(request.ClusterID)
	encoder.WriteInt32(request.BrokerID)
	encoder.WriteStringWithUint16Length(request.Host)
	encoder.WriteInt32(request.Port)
	encoder.WriteStringWithUint16Length(request.LogDir)
	return encoder.GetBytes()
}

func (request *RegisterBrokerAndFetchMetadataRequest) Decode(data []byte) error {
	decoder := protocol.NewDecoder(data)
	*request = RegisterBrokerAndFetchMetadataRequest{
		ClusterID: decoder.ReadStringWithUint16Length(),
		BrokerID:  decoder.ReadInt32(),
		Host:      decoder.ReadStringWithUint16Length(),
		Port:      decoder.ReadInt32(),
		LogDir:    decoder.ReadStringWithUint16Length(),
	}
	return decoder.GetError()
}

func (response *RegisterBrokerAndFetchMetadataResponse) Encode() ([]byte, error) {
	return encodeStartupResponse(response.Error, response.Image)
}

func (response *RegisterBrokerAndFetchMetadataResponse) Decode(data []byte) error {
	errEnvelope, image, err := decodeStartupResponse(data)
	if err != nil {
		return err
	}

	*response = RegisterBrokerAndFetchMetadataResponse{
		Error: errEnvelope,
		Image: image,
	}
	return nil
}

func (request *FetchMetadataImageRequest) Encode() ([]byte, error) {
	encoder := protocol.NewEncoder()
	encoder.WriteStringWithUint16Length(request.ClusterID)
	encoder.WriteInt32(request.NodeID)
	return encoder.GetBytes()
}

func (request *FetchMetadataImageRequest) Decode(data []byte) error {
	decoder := protocol.NewDecoder(data)
	*request = FetchMetadataImageRequest{
		ClusterID: decoder.ReadStringWithUint16Length(),
		NodeID:    decoder.ReadInt32(),
	}
	return decoder.GetError()
}

func (response *FetchMetadataImageResponse) Encode() ([]byte, error) {
	return encodeStartupResponse(response.Error, response.Image)
}

func (response *FetchMetadataImageResponse) Decode(data []byte) error {
	errEnvelope, image, err := decodeStartupResponse(data)
	if err != nil {
		return err
	}

	*response = FetchMetadataImageResponse{
		Error: errEnvelope,
		Image: image,
	}
	return nil
}

func encodeStartupResponse(errEnvelope Error, image *metadata.Image) ([]byte, error) {
	encoder := protocol.NewEncoder()
	encodeError(encoder, errEnvelope)
	encodeImage(encoder, image)
	return encoder.GetBytes()
}

func decodeStartupResponse(data []byte) (Error, *metadata.Image, error) {
	decoder := protocol.NewDecoder(data)
	errEnvelope := decodeError(decoder)
	image := decodeImage(decoder)
	if err := decoder.GetError(); err != nil {
		return Error{}, nil, err
	}
	return errEnvelope, image, nil
}

func encodeError(encoder *protocol.Encoder, errEnvelope Error) {
	encoder.WriteInt16(int16(errEnvelope.Code))
	encoder.WriteStringWithUint16Length(errEnvelope.Message)
	encoder.WriteBool(errEnvelope.LeaderController != nil)
	if errEnvelope.LeaderController == nil {
		return
	}

	encoder.WriteInt32(errEnvelope.LeaderController.NodeID)
	encoder.WriteStringWithUint16Length(errEnvelope.LeaderController.Host)
	encoder.WriteInt32(errEnvelope.LeaderController.Port)
}

func decodeError(decoder *protocol.Decoder) Error {
	errEnvelope := Error{
		Code:    ErrorCode(decoder.ReadInt16()),
		Message: decoder.ReadStringWithUint16Length(),
	}

	if !decoder.ReadBool() {
		return errEnvelope
	}

	errEnvelope.LeaderController = &ControllerEndpoint{
		NodeID: decoder.ReadInt32(),
		Host:   decoder.ReadStringWithUint16Length(),
		Port:   decoder.ReadInt32(),
	}
	return errEnvelope
}

func encodeImage(encoder *protocol.Encoder, image *metadata.Image) {
	encoder.WriteBool(image != nil)
	if image == nil {
		return
	}

	encoder.WriteStringWithUint16Length(image.ClusterID)
	encoder.WriteInt64(image.AppliedOffset)

	topicNames := make([]string, 0, len(image.Topics))
	for topicName := range image.Topics {
		topicNames = append(topicNames, topicName)
	}
	sort.Strings(topicNames)

	encoder.WriteUint32(uint32(len(topicNames)))
	for _, topicName := range topicNames {
		topic := image.Topics[topicName]
		encoder.WriteStringWithUint16Length(topic.Name)
		encoder.WriteInt32(topic.Config.Partitions)
		encoder.WriteInt16(topic.Config.ReplicationFactor)
		encoder.WriteInt64(topic.Config.RetentionMs)
		encoder.WriteInt64(topic.Config.RetentionBytes)
		encoder.WriteInt64(topic.CreatedAt.UnixMilli())
	}

	partitionKeys := make([]metadata.PartitionKey, 0, len(image.Partitions))
	for key := range image.Partitions {
		partitionKeys = append(partitionKeys, key)
	}
	sort.Slice(partitionKeys, func(i, j int) bool {
		if partitionKeys[i].TopicName == partitionKeys[j].TopicName {
			return partitionKeys[i].PartitionID < partitionKeys[j].PartitionID
		}
		return partitionKeys[i].TopicName < partitionKeys[j].TopicName
	})

	encoder.WriteUint32(uint32(len(partitionKeys)))
	for _, key := range partitionKeys {
		partition := image.Partitions[key]
		encoder.WriteStringWithUint16Length(partition.TopicName)
		encoder.WriteInt32(partition.PartitionID)
		encoder.WriteInt32(partition.LeaderBrokerID)

		encoder.WriteUint32(uint32(len(partition.ReplicaBrokerIDs)))
		for _, brokerID := range partition.ReplicaBrokerIDs {
			encoder.WriteInt32(brokerID)
		}

		encoder.WriteUint32(uint32(len(partition.ISRBrokerIDs)))
		for _, brokerID := range partition.ISRBrokerIDs {
			encoder.WriteInt32(brokerID)
		}

		encoder.WriteInt64(partition.CreatedAt.UnixMilli())
		encoder.WriteInt64(partition.UpdatedAt.UnixMilli())
	}

	brokerIDs := make([]int32, 0, len(image.Brokers))
	for brokerID := range image.Brokers {
		brokerIDs = append(brokerIDs, brokerID)
	}
	sort.Slice(brokerIDs, func(i, j int) bool {
		return brokerIDs[i] < brokerIDs[j]
	})

	encoder.WriteUint32(uint32(len(brokerIDs)))
	for _, brokerID := range brokerIDs {
		broker := image.Brokers[brokerID]
		encoder.WriteInt32(broker.BrokerID)
		encoder.WriteStringWithUint16Length(broker.Host)
		encoder.WriteInt32(broker.Port)
		encoder.WriteStringWithUint16Length(broker.LogDir)
		encoder.WriteInt64(broker.RegisteredAt.UnixMilli())
		encoder.WriteInt64(broker.UpdatedAt.UnixMilli())
	}
}

func decodeImage(decoder *protocol.Decoder) *metadata.Image {
	if !decoder.ReadBool() {
		return nil
	}

	image := &metadata.Image{
		ClusterID:     decoder.ReadStringWithUint16Length(),
		AppliedOffset: decoder.ReadInt64(),
		Topics:        make(map[string]metadata.TopicMetadata),
		Partitions:    make(map[metadata.PartitionKey]metadata.PartitionMetadata),
		Brokers:       make(map[int32]metadata.BrokerMetadata),
	}

	topicCount := decoder.ReadUint32()
	for i := uint32(0); i < topicCount; i++ {
		topic := metadata.TopicMetadata{
			Name: decoder.ReadStringWithUint16Length(),
			Config: metadata.TopicConfig{
				Partitions:        decoder.ReadInt32(),
				ReplicationFactor: decoder.ReadInt16(),
				RetentionMs:       decoder.ReadInt64(),
				RetentionBytes:    decoder.ReadInt64(),
			},
			CreatedAt: time.UnixMilli(decoder.ReadInt64()),
		}
		image.Topics[topic.Name] = topic
	}

	partitionCount := decoder.ReadUint32()
	for i := uint32(0); i < partitionCount; i++ {
		partition := metadata.PartitionMetadata{
			TopicName:      decoder.ReadStringWithUint16Length(),
			PartitionID:    decoder.ReadInt32(),
			LeaderBrokerID: decoder.ReadInt32(),
		}

		replicaCount := decoder.ReadUint32()
		partition.ReplicaBrokerIDs = make([]int32, replicaCount)
		for j := uint32(0); j < replicaCount; j++ {
			partition.ReplicaBrokerIDs[j] = decoder.ReadInt32()
		}

		isrCount := decoder.ReadUint32()
		partition.ISRBrokerIDs = make([]int32, isrCount)
		for j := uint32(0); j < isrCount; j++ {
			partition.ISRBrokerIDs[j] = decoder.ReadInt32()
		}

		partition.CreatedAt = time.UnixMilli(decoder.ReadInt64())
		partition.UpdatedAt = time.UnixMilli(decoder.ReadInt64())

		key := metadata.PartitionKey{
			TopicName:   partition.TopicName,
			PartitionID: partition.PartitionID,
		}
		image.Partitions[key] = partition
	}

	brokerCount := decoder.ReadUint32()
	for i := uint32(0); i < brokerCount; i++ {
		broker := metadata.BrokerMetadata{
			BrokerID:     decoder.ReadInt32(),
			Host:         decoder.ReadStringWithUint16Length(),
			Port:         decoder.ReadInt32(),
			LogDir:       decoder.ReadStringWithUint16Length(),
			RegisteredAt: time.UnixMilli(decoder.ReadInt64()),
			UpdatedAt:    time.UnixMilli(decoder.ReadInt64()),
		}
		image.Brokers[broker.BrokerID] = broker
	}

	return image
}