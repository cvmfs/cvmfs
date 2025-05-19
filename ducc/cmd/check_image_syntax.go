package cmd

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/cvmfs/ducc/lib"
)

var (
	machineFriendly bool
)

func init() {
	checkImageSyntaxCmd.Flags().BoolVarP(&machineFriendly, "machine-friendly", "z", false, "produce machine friendly output, one line of csv")
	rootCmd.AddCommand(checkImageSyntaxCmd)
}

var checkImageSyntaxCmd = &cobra.Command{
	Use:     "check-image-syntax",
	Short:   "Check that the provide image has a valid syntax, the same checks are applied before any command in the converter.",
	Aliases: []string{"check-image", "image-check"},
	Args:    cobra.MinimumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) (err error) {
		img, err := lib.ParseImage(args[0])
		if err != nil {
			fmt.Println(err)
			return err
		}
		img.PrintImage(machineFriendly, true)
		return nil
	},
}
