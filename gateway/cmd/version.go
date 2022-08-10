package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

var Version = "development"

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print CVMFS gateway version number",
	Long:  "Print CVMFS gateway version number",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("CernVM-FS Gateway Server Version:\t", Version)
	},
}

func init() {
	rootCmd.AddCommand(versionCmd)
}
