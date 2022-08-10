package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	gw "github.com/cvmfs/gateway/internal/gateway"
	be "github.com/cvmfs/gateway/internal/gateway/backend"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var repository string

var listLeasesCmd = &cobra.Command{
	Use:   "list-leases",
	Short: "List current active leases",
	Long:  "List current active leases",
	Run: func(cmd *cobra.Command, args []string) {
		gw.SetLogLevel(gw.LogError)
		if err := executeListLeases(); err != nil {
			gw.Log("cmd-list-leases", gw.LogError).
				Err(err).
				Msg("list-leases command failed")
			os.Exit(1)
		}
	},
}

func init() {
	rootCmd.AddCommand(listLeasesCmd)
	listLeasesCmd.Flags().StringVarP(
		&repository,
		"repository",
		"r",
		"",
		"retrieve leases for this repository; retrieve leases for all repositories if empty")
	viper.BindPFlag("repository", listLeasesCmd.Flags().Lookup("repository"))
}

func executeListLeases() error {
	services, err := be.StartBackend(*Config, false)
	if err != nil {
		return fmt.Errorf("could not start backend services: %w", err)
	}
	defer services.Stop()

	var leases map[string]be.LeaseDTO
	if repository == "" {
		leases, err = services.GetLeases(context.Background())
		if err != nil {
			return fmt.Errorf("could not retrieve leases: %w", err)
		}
	} else {
		leases, err = services.GetLeasesByRepository(context.Background(), repository)
		if err != nil {
			return fmt.Errorf("could not retrieve leases: %w", err)
		}
	}

	leasesResult := make([]be.LeaseDTO, 0)
	for _, l := range leases {
		leasesResult = append(leasesResult, l)
	}

	jsonBytes, err := json.MarshalIndent(leasesResult, "", "  ")
	if err != nil {
		return fmt.Errorf("could not format result: %w", err)
	}

	fmt.Println(string(jsonBytes))

	return nil
}
