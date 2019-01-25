package cmd

import (
	"time"

	"github.com/spf13/cobra"

	"github.com/cvmfs/ducc/lib"
)

var rootCmd = &cobra.Command{
	Use:   "repository-manager",
	Short: "Show the several commands available.",
	Run: func(cmd *cobra.Command, args []string) {
		cmd.Help()
	},
}

func EntryPoint() {
	rootCmd.Execute()
}

func AliveMessage() {
	ticker := time.NewTicker(30 * time.Second)
	go func() {
		for _ = range ticker.C {
			lib.Log().Info("Process alive")
		}
	}()
}
