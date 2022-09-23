package cmd

import (
	"context"
	"fmt"
	"os"
	"time"

	gw "github.com/cvmfs/gateway/internal/gateway"
	be "github.com/cvmfs/gateway/internal/gateway/backend"

	"github.com/spf13/cobra"
)

const LeaseQueryWait = 5 * time.Second

var flushLeasesCmd = &cobra.Command{
	Use:   "flush-leases repository",
	Short: "Flush all active leases for repository",
	Long:  "Flush all active leases for repository",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		gw.SetLogLevel(gw.LogError)
		if err := executeFlushLeases(args[0]); err != nil {
			gw.Log("cmd-flush-leases", gw.LogError).
				Err(err).
				Msg("flush-leases command failed")
			os.Exit(1)
		}
	},
}

func init() {
	rootCmd.AddCommand(flushLeasesCmd)
}

func executeFlushLeases(repository string) error {
	services, err := be.StartBackend(*Config, false)
	if err != nil {
		return fmt.Errorf("could not start backend services: %w", err)
	}
	defer services.Stop()

	fmt.Printf("Flush active leases for repository: %v\n", repository)

	for {
		leases, err := services.GetLeasesByRepository(context.Background(), repository)
		if err != nil {
			return fmt.Errorf("could not retrieve leases: %w", err)
		}
		fmt.Printf("Current active leases: %v\n", len(leases))

		if len(leases) == 0 {
			fmt.Printf("No more active leases\n")
			break
		}

		fmt.Printf("Waiting...\n")
		time.Sleep(LeaseQueryWait)
	}

	return nil
}
