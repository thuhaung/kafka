package broker

import (
	"context"
	"log"

	appconfig "github.com/thuhaung/kafka/internal/config"
	"github.com/thuhaung/kafka/internal/network"
	"github.com/thuhaung/kafka/internal/network/handlers"
	nodeconfig "github.com/thuhaung/kafka/internal/broker/config"
)

type Options struct {
	BootstrapServer string
	ConfigPath string
}

func NewBroker(nodeConfig *nodeconfig.NodeConfig) *Broker {
	return &Broker{
		nodeConfig: nodeConfig,
		server: network.NewServer(
			appconfig.MAX_CONNECTIONS,
			&network.KafkaTransport{},
			&handlers.BrokerHandler{},
		),
	}
}

func Run(ctx context.Context, opts Options) error {
	nodeConfig, err := nodeconfig.ParseConfig(opts.ConfigPath)
	if err != nil {
		return err
	}

	broker := NewBroker(nodeConfig)

	if err := broker.server.Start(ctx, opts.BootstrapServer); err != nil {
		log.Fatal(err)
	}

	broker.server.Wait()
	log.Println("Server stopped")

	return nil
}
