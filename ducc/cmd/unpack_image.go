package cmd

import (
	"os"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	"github.com/cvmfs/ducc/lib"
)

func init() {
	rootCmd.AddCommand(unpackImageCmd)
}

var unpackImageCmd = &cobra.Command{
	Use:   "unpack-image image_to_convert cvmfs_repository",
	Short: "Convert a single image",
	Args:  cobra.MinimumNArgs(2),
	Run: func(cmd *cobra.Command, args []string) {

		imageRef := args[0]
		cvmfsRepo := args[1]
		image, err := lib.ParseImage(imageRef)
		if err != nil {
			lib.LogE(err).WithFields(log.Fields{"input image": imageRef}).Error("Error in parsing the image")
			os.Exit(1)
		}
		lib.Log().WithFields(log.Fields{"image": imageRef, "cvmfs repo": cvmfsRepo}).Info("Unpacking the image in the output direcotry")
		err = image.UnpackFlatFilesystemInDir(cvmfsRepo)
		if err != nil {
			lib.LogE(err).WithFields(log.Fields{"input image": imageRef, "cvmfs repo": cvmfsRepo}).Error("Error in unpacking the image in the output directory")
			os.Exit(1)
		}
	},
}
