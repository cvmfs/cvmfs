package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

const (
	deleteBatch = 50
)

var (
	dryRun bool
)

func init() {
	garbageCollectionCmd.Flags().BoolVar(&dryRun, "dry-run", false, "Dry run the garbage collection")
	rootCmd.AddCommand(garbageCollectionCmd)
}

var garbageCollectionCmd = &cobra.Command{
	Use:     "garbage-collection",
	Short:   "Removes layers that are not necessary anymore",
	Aliases: []string{"gc"},
	Args:    cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("Garbage collection is not implemented yet\n")
		// TODO: Implement garbage collection
	},
}
