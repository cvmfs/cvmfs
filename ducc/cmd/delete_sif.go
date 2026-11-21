package cmd

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/cvmfs/ducc/lib"
)

var deleteSIFDryRun bool

func init() {
	deleteSIFCmd.Flags().BoolVar(&deleteSIFDryRun, "dry-run", false,
		"Print what would be deleted without making any changes")
	rootCmd.AddCommand(deleteSIFCmd)
}

var deleteSIFCmd = &cobra.Command{
	Use:   "delete-sif <cvmfs-repo> <name>",
	Short: "Delete the public link sif/<name> of a SIF image",
	Long: `Deletes the symlink sif/<name> created by convert-sif or a recipe's
sif_input. The image under .flat/ and its provenance file under .sifinfo/
are removed by the next garbage-collection run once no link points to them.
Remove the entry from the recipe first, or the next conversion recreates it.`,
	Args: cobra.ExactArgs(2),
	RunE: func(cmd *cobra.Command, args []string) error {
		deleted, err := lib.DeleteSIFFromCVMFS(args[0], args[1], deleteSIFDryRun)
		if err != nil {
			return err
		}
		if !deleted {
			fmt.Printf("sif/%s not found in CVMFS\n", args[1])
		}
		return nil
	},
}
