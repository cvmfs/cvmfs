package cmd

import (
	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(loopCmd)
}

var loopCmd = &cobra.Command{
	Deprecated: "Use the daemon instead",
}
