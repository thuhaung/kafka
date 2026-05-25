package metadata

import (
	"time"

	"github.com/thuhaung/kafka/internal/protocol"
)

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
	RetentionBytes	 int64
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
	// TODO: Should store active segment?
}

type BrokerMetadata struct {
	BrokerID       int32
	Host           string
	Port           int32
	LogDir         string
	RegisteredAt   time.Time
	UpdatedAt      time.Time
}

type BrokerRegistrationRecord struct {
	ClusterID    string
	BrokerID     int32
	Host         string
	Port         int32
	LogDir       string
	RegisteredAt time.Time
}

func (r *BrokerRegistrationRecord) Encode() ([]byte, error) {
	encoder := protocol.NewEncoder()

	encoder.WriteStringWithUint16Length(r.ClusterID)
	encoder.WriteInt32(r.BrokerID)
	encoder.WriteStringWithUint16Length(r.Host)
	encoder.WriteInt32(r.Port)
	encoder.WriteStringWithUint16Length(r.LogDir)
	encoder.WriteInt64(r.RegisteredAt.UnixMilli())

	return encoder.GetBytes()
}

func (r *BrokerRegistrationRecord) Decode(data []byte) error {
	decoder := protocol.NewDecoder(data)

	clusterID := decoder.ReadStringWithUint16Length()
	brokerID := decoder.ReadInt32()
	host := decoder.ReadStringWithUint16Length()
	port := decoder.ReadInt32()
	logDir := decoder.ReadStringWithUint16Length()
	registeredAtMs := decoder.ReadInt64()

	if err := decoder.GetError(); err != nil {
		return err
	}

	r.ClusterID = clusterID
	r.BrokerID = brokerID
	r.Host = host
	r.Port = port
	r.LogDir = logDir
	r.RegisteredAt = time.UnixMilli(registeredAtMs)

	return nil
}

func (r *BrokerRegistrationRecord) toBrokerMetadata() BrokerMetadata {
	return BrokerMetadata{
		BrokerID: r.BrokerID,
		Host: r.Host,
		Port: r.Port,
		LogDir: r.LogDir,
		RegisteredAt: r.RegisteredAt,
	}
}
