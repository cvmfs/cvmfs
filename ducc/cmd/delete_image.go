package cmd

import (
	"fmt"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	"github.com/cvmfs/ducc/lib"
	l "github.com/cvmfs/ducc/log"
)

var deleteImageDryRun bool

func init() {
	deleteImageCmd.Flags().BoolVar(&deleteImageDryRun, "dry-run", false,
		"Print what would be deleted without making any changes")
	rootCmd.AddCommand(deleteImageCmd)
}

var deleteImageCmd = &cobra.Command{
	Use:   "delete-image <cvmfs-repo> <image>",
	Short: "Delete the user-facing paths for an image from the CVMFS repository",
	Long: `Deletes the user-facing symlink and manifest for an image from the CVMFS
repository, making it eligible for garbage collection on the next GC run.

The image argument must be a full image reference, for example:
  https://registry.hub.docker.com/library/ubuntu:22.04`,
	Args: cobra.ExactArgs(2),
	RunE: func(cmd *cobra.Command, args []string) error {
		CVMFSRepo := args[0]
		imageRef := args[1]

		img, err := lib.ParseImage(imageRef)
		if err != nil {
			l.LogE(err).WithFields(log.Fields{"image": imageRef}).
				Error("Failed to parse image reference")
			return err
		}

		deleted, err := lib.DeleteImageFromCVMFS(CVMFSRepo, &img, deleteImageDryRun)
		if err != nil {
			l.LogE(err).WithFields(log.Fields{"image": img.GetSimpleName()}).
				Error("Failed to delete image from CVMFS")
			return err
		}
		if !deleted {
			fmt.Printf("Image %s not found in CVMFS\n", img.GetSimpleName())
		}
		return nil
	},
}
