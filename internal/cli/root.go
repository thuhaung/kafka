package cli

import (
	"errors"

	"github.com/spf13/cobra"
)

var bootstrapServer string

const (
	bootstrapServerFlag = "bootstrap-server"
)

var (
	ErrBootstrapServerRequired = errors.New("Bootstrap server is required. Provide it using --" + bootstrapServerFlag)
)

func NewRootCommand() *cobra.Command {
	rootCmd := &cobra.Command{
		Use: "kafka",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			if bootstrapServer == "" {
				return ErrBootstrapServerRequired
			}
			return nil
		},
	}

	rootCmd.PersistentFlags().StringVar(
		&bootstrapServer,
		bootstrapServerFlag,
		"", 
		"Bootstrap server for client to make initial connection",
	)

	rootCmd.AddCommand(NewServerCommand(&bootstrapServer))

	return rootCmd
}
