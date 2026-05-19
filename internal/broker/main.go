package main

import (
	"context"
	"os/signal"
	"syscall"
	"log"

	"github.com/thuhaung/kafka/internal/config"
	"github.com/thuhaung/kafka/internal/protocol/codec"
	"github.com/thuhaung/kafka/internal/protocol/handlers"
	"github.com/thuhaung/kafka/internal/protocol/server"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)

	defer stop()

	server := server.NewServer(config.MAX_CONNECTIONS, &codec.KafkaCodec{}, &handlers.BrokerHandler{})
	if err := server.Start(ctx, "9092"); err != nil {
		log.Fatal(err)
	}

	server.Wait()
	log.Println("Server stopped")
}
