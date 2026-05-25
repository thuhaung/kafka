package metadata

import (
	"fmt"
	"log"
	"path/filepath"
	"strings"
	"sync/atomic"

	"github.com/thuhaung/kafka/internal/config"
	"github.com/thuhaung/kafka/internal/storage"
	"github.com/thuhaung/kafka/internal/storage/segment"
)

const (
	TopicCreationRecordType      uint8 = 1
	PartitionCreationRecordType  uint8 = 2
	PartitionUpdateRecordType    uint8 = 3
	BrokerRegistrationRecordType uint8 = 4
)

type RecordEnvelope struct {
	Type uint8
	Data []byte
}

var currentImage atomic.Pointer[Image]

func NewEmptyImage(clusterID string) *Image {
	return &Image{
		ClusterID:     clusterID,
		AppliedOffset: -1,
		Topics:        make(map[string]TopicMetadata),
		Partitions:    make(map[PartitionKey]PartitionMetadata),
		Brokers:       make(map[int32]BrokerMetadata),
	}
}

func LoadImage() *Image {
	return CloneImage(currentImage.Load())
}

func (image *Image) BuildImage(logDir string) error {
	partition := fmt.Sprintf("%s-%d", config.CLUSTER_METADATA_TOPIC, config.CLUSTER_METADATA_TOPIC_PARTITION)
	partitionPath := filepath.Join(logDir, config.CLUSTER_METADATA_TOPIC, partition)

	fileInfos, err := storage.GetFilesInDir(partitionPath)
	if err != nil {
		return err
	}

	offset := int64(0)
	for _, fileInfo := range fileInfos {
		if !strings.HasSuffix(fileInfo.Name(), ".log") {
			continue
		}

		position := int64(0)
		filePath := filepath.Join(partitionPath, fileInfo.Name())
		log.Printf("Processing log file %s starting at offset %d", filePath, offset)

		file, err := segment.OpenLogFile(filePath)
		if err != nil {
			return err
		}

		for position < fileInfo.Size() {
			recordHeader, err := segment.ReadRecordHeaderAt(file, position)
			if err != nil {
				file.Close()
				return err
			}

			err = segment.ValidateRecordHeader(offset, position, fileInfo.Size(), recordHeader)
			if err != nil {
				file.Close()
				return err
			}

			record, err := segment.ReadRecordAt(file, position, recordHeader.Length)
			if err != nil {
				file.Close()
				return err
			}

			if err := image.applyChanges(record); err != nil {
				file.Close()
				return err
			}

			image.AppliedOffset = offset
			offset++
			position += int64(recordHeader.Length) + segment.HeaderSize
		}

		if err := file.Close(); err != nil {
			return err
		}
	}

	return nil
}

func PublishImage(image *Image) {
	currentImage.Store(CloneImage(image))
}

func CloneImage(image *Image) *Image {
	if image == nil {
		return nil
	}

	clonedImage := &Image{
		ClusterID:     image.ClusterID,
		AppliedOffset: image.AppliedOffset,
		Topics:        make(map[string]TopicMetadata, len(image.Topics)),
		Partitions:    make(map[PartitionKey]PartitionMetadata, len(image.Partitions)),
		Brokers:       make(map[int32]BrokerMetadata, len(image.Brokers)),
	}

	for name, topic := range image.Topics {
		clonedImage.Topics[name] = topic
	}

	for key, partition := range image.Partitions {
		clonedPartition := partition
		clonedPartition.ReplicaBrokerIDs = append([]int32(nil), partition.ReplicaBrokerIDs...)
		clonedPartition.ISRBrokerIDs = append([]int32(nil), partition.ISRBrokerIDs...)
		clonedImage.Partitions[key] = clonedPartition
	}

	for brokerID, broker := range image.Brokers {
		clonedImage.Brokers[brokerID] = broker
	}

	return clonedImage
}
