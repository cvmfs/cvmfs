package cmd

import (
	"context"
	"fmt"
	"os"

	gw "github.com/cvmfs/gateway/internal/gateway"
	be "github.com/cvmfs/gateway/internal/gateway/backend"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var cancelPath string

var cancelLeaseCmd = &cobra.Command{
	Use:   "cancel-lease",
	Short: "Cancel one or multiple leases",
	Long:  "Cancel a specific lease, all leases for a repository, or all leases across all repositories",
	Run: func(cmd *cobra.Command, args []string) {
		gw.SetLogLevel(gw.LogError)
		if err := executeCancelLease(); err != nil {
			gw.Log("cmd-cancel-lease", gw.LogError).
				Err(err).
				Msg("cancel-lease command failed")
			os.Exit(1)
		}
	},
}

func init() {
	rootCmd.AddCommand(cancelLeaseCmd)
	cancelLeaseCmd.Flags().StringVarP(
		&cancelPath,
		"path",
		"p",
		"",
		"cancel all leases below this repository path (ex: sft.cern.ch/)")
	viper.BindPFlag("cancel-lease-path", cancelLeaseCmd.Flags().Lookup("path"))
}

func executeCancelLease() error {
	services, err := be.StartBackend(*Config, false)
	if err != nil {
		return fmt.Errorf("could not start backend services: %w", err)
	}
	defer services.Stop()

	if cancelPath == "" {
		return fmt.Errorf("lease path to cancel must not be empty")
	}

	if err := services.CancelLeases(context.Background(), cancelPath); err != nil {
		return fmt.Errorf("could not cancel leases: %w", err)
	}

	fmt.Printf("Leases cancelled\n")

	return nil
}
