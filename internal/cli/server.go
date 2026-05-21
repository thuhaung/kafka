package cli

import (
	"context"
	"errors"
	"log"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
	"github.com/thuhaung/kafka/internal/broker"
)

const (
	configPathFlag = "config-path"
)

var (
	ErrMissingConfigPath = errors.New("Config path is required. Provide it using --" + configPathFlag)
)

func NewServerCommand(bootstrapServer *string) *cobra.Command {
	var configPath string

	serverCmd := &cobra.Command{
		Use: "server",
		RunE: func(cmd *cobra.Command, args []string) error {
			log.Println("Starting Kafka server...")

			if configPath == "" {
				return ErrMissingConfigPath
			}

			log.Println("Bootstrap server:", *bootstrapServer)
			log.Println("Using config path:", configPath)

			ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
			defer stop()

			return broker.Run(ctx, broker.Options{
				BootstrapServer: *bootstrapServer,
				ConfigPath: configPath,
			})
		},
	}

	serverCmd.Flags().StringVar(
		&configPath,
		configPathFlag, 
		"", 
		"Path to the server configuration file",
	)

	return serverCmd
}
