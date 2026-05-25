package broker

import (
	"github.com/thuhaung/kafka/internal/broker/config"
	"github.com/thuhaung/kafka/internal/network"
)

type Broker struct {
	nodeConfig *config.NodeConfig
	servers []*network.Server
}
