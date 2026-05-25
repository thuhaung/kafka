package metadata

import (
	"errors"
	"fmt"
	"log"

	"github.com/thuhaung/kafka/internal/protocol"
)

var (
	ErrUnknownRecordType = errors.New("Unknown record type")
	ErrCannotApplyChange = errors.New("Cannot apply change to image")
)

func (image *Image) applyChanges(record []byte) error {
	decoder := protocol.NewDecoder(record)
	recordType := decoder.ReadUint8()

	if err := decoder.GetError(); err != nil {
		return err
	}

	log.Printf("Applying record of type %d to image", recordType)

	switch recordType {
		case TopicCreationRecordType:
			err := image.applyTopicCreation(record)
			if err != nil {
				return fmt.Errorf("%w: %v", ErrCannotApplyChange, err)
			}
		case PartitionCreationRecordType:
			err := image.applyPartitionCreation(record)
			if err != nil {
				return fmt.Errorf("%w: %v", ErrCannotApplyChange, err)
			}
		case PartitionUpdateRecordType:
			err := image.applyPartitionUpdate(record)
			if err != nil {
				return fmt.Errorf("%w: %v", ErrCannotApplyChange, err)
			}
		case BrokerRegistrationRecordType:
			err := image.applyBrokerRegistration(record)
			if err != nil {
				return fmt.Errorf("%w: %v", ErrCannotApplyChange, err)
			}
		default:
			return ErrUnknownRecordType
	}

	return nil
}

func (image *Image) applyBrokerRegistration(record []byte) error {
	brokerRegistrationRecord := &BrokerRegistrationRecord{}
	if err := brokerRegistrationRecord.Decode(record); err != nil {
		return err
	}

	if brokerRegistrationRecord.ClusterID != image.ClusterID {
		return fmt.Errorf("Cluster ID mismatch: expected %s but got %s", image.ClusterID, brokerRegistrationRecord.ClusterID)
		
	}

	image.Brokers[brokerRegistrationRecord.BrokerID] = brokerRegistrationRecord.toBrokerMetadata()

	return nil
}

func (image *Image) applyTopicCreation(record []byte) error {
	return nil
}

func (image *Image) applyPartitionCreation(record []byte) error {
	return nil
}

func (image *Image) applyPartitionUpdate(record []byte) error {
	return nil
}
