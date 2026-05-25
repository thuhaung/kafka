package broker

import (
	"errors"
	"fmt"
	"log"

	"github.com/thuhaung/kafka/internal/metadata"
)

var (
	ErrFailedToReplayMetadata = errors.New("Failed to replay metadata changes from log")
)

func (broker *Broker) startController() error {
	log.Printf("Replaying metadata log for broker of node ID %d", broker.nodeConfig.NodeID)

	image := metadata.NewEmptyImage(broker.nodeConfig.ClusterID)

	if err := image.BuildImage(broker.nodeConfig.LogDir); err != nil {
		return fmt.Errorf("%w: %v", ErrFailedToReplayMetadata, err)
	}

	metadata.PublishImage(image)

	log.Printf("Successfully built and published metadata image for broker of node ID %d", broker.nodeConfig.NodeID)
	return nil
}
