package broker

import (
	"context"
	"log"

	"github.com/thuhaung/kafka/internal/config"
	"github.com/thuhaung/kafka/internal/network"
	"github.com/thuhaung/kafka/internal/network/handlers"
)

type Options struct {
	BootstrapServer string
	ConfigPath string
}

func Run(ctx context.Context, opts Options) error {
	// parse config
	server := network.NewServer(config.MAX_CONNECTIONS, &network.KafkaTransport{}, &handlers.BrokerHandler{})
	if err := server.Start(ctx, opts.BootstrapServer); err != nil {
		log.Fatal(err)
	}

	server.Wait()
	log.Println("Server stopped")

	return nil
}
