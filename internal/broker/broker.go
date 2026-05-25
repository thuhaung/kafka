package broker

import (
	"context"
	"fmt"
	"log"

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

	log.Printf("Parsed broker config: %v", nodeConfig)
	log.Printf("Starting broker with node ID %d, cluster ID %s, and role %s", nodeConfig.NodeID, nodeConfig.ClusterID, nodeConfig.Role)

	broker := NewBroker(nodeConfig)

	if broker.nodeConfig.Role == nodeconfig.RoleController {
		if err := broker.startController(); err != nil {
			return err
		}
	}

	for i, listener := range nodeConfig.ListenerConfigs {
		log.Printf("Starting listener %s on %s: %d for broker of node ID %d", listener.Type, listener.Host, listener.Port, nodeConfig.NodeID)
		addr := fmt.Sprintf("%s:%d", listener.Host, listener.Port)

		broker.servers[i] = network.NewServer(
			appconfig.MAX_CONNECTIONS,
			&network.KafkaTransport{},
			&handlers.BrokerHandler{},
		)

		if err := broker.servers[i].Start(ctx, addr); err != nil {
			return err
		}
	}

	for _, server := range broker.servers {
		server.Wait()
	}

	return nil
}
