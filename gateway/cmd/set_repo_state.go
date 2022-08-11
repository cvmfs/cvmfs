package cmd

import (
	"context"
	"fmt"
	"os"

	gw "github.com/cvmfs/gateway/internal/gateway"
	be "github.com/cvmfs/gateway/internal/gateway/backend"
	"github.com/spf13/cobra"
)

var setRepoStateCmd = &cobra.Command{
	Use:   "set-repo-state repository state",
	Short: "Set the state of a repository",
	Long:  "Set the state of a repository (enabled, disabled, locked)",
	Args: func(cmd *cobra.Command, args []string) error {
		if len(args) != 2 {
			return fmt.Errorf("accepts 2 args, received, %v", len(args))
		}

		_, present := be.RepoStates[args[1]]

		if !present {
			return fmt.Errorf("invalid state: %v", args[1])
		}

		return nil
	},
	Run: func(cmd *cobra.Command, args []string) {
		gw.SetLogLevel(gw.LogError)
		if err := executeSetRepoState(args[0], be.RepoStates[args[1]]); err != nil {
			gw.Log("cmd-set-repo-state", gw.LogError).
				Err(err).
				Msg("set-repo-state command failed")
			os.Exit(1)
		}
	},
}

func init() {
	rootCmd.AddCommand(setRepoStateCmd)
}

func executeSetRepoState(repository string, state be.RepoState) error {
	services, err := be.StartBackend(*Config, false)
	if err != nil {
		return fmt.Errorf("could not start backend services: %w", err)
	}
	defer services.Stop()

	if err = services.SetRepoState(context.Background(), repository, state); err != nil {
		return fmt.Errorf("could not set state %v for repository %v: %w", state, repository, err)
	}

	return nil
}
