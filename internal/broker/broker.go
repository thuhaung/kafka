package broker

import (
	"context"
	"fmt"
	"log"
	"sync"

	nodeconfig "github.com/thuhaung/kafka/internal/broker/config"
	appconfig "github.com/thuhaung/kafka/internal/config"
	"github.com/thuhaung/kafka/internal/network"
	"github.com/thuhaung/kafka/internal/network/handlers"
)

type Options struct {
	BootstrapServer string
	ConfigPath string
}

func NewBroker(nodeConfig *nodeconfig.NodeConfig) *Broker {
	return &Broker{
		nodeConfig: nodeConfig,
		servers: make([]*network.Server, len(nodeConfig.ListenerConfigs)),
	}
}

func Run(ctx context.Context, opts Options) error {
	nodeConfig, err := nodeconfig.ParseConfig(opts.ConfigPath)
	if err != nil {
		return err
	}

	broker := NewBroker(nodeConfig)
	var wg sync.WaitGroup

	for i, listener := range nodeConfig.ListenerConfigs {
		log.Printf("Starting listener %s on %s:%d", listener.Type, listener.Host, listener.Port)
		addr := fmt.Sprintf("%s:%d", listener.Host, listener.Port)
		wg.Add(1)

		broker.servers[i] = network.NewServer(
			appconfig.MAX_CONNECTIONS,
			&network.KafkaTransport{},
			&handlers.BrokerHandler{},
		)

		if err := broker.servers[i].Start(ctx, addr); err != nil {
			wg.Done()
			return err
		}
	}

	for _, server := range broker.servers {
		server.Wait()
	}

	return nil
}
