package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

var version = "0.1.4"

func init() {
	rootCmd.AddCommand(versionCmd)
}

var versionCmd = &cobra.Command{
	Use:     "version",
	Short:   "Print the version of ducc",
	Aliases: []string{"v"},
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println(version)
	},
}
