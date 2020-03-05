package cmd

import (
	"os"
	"path/filepath"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	"github.com/cvmfs/ducc/lib"
)

func init() {
	rootCmd.AddCommand(unpackImageCmd)
}

var unpackImageCmd = &cobra.Command{
	Use:   "unpack-image image_to_convert [dir to unpack into]",
	Short: "Convert a single image",
	Args:  cobra.MinimumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {

		imageRef := args[0]
		var outputDirInput string
		if len(args) > 0 {
			outputDirInput = args[1]
		} else {
			outputDirInput = "./"
		}
		outputDir, err := filepath.Abs(outputDirInput)
		if err != nil {
			lib.LogE(err).WithFields(log.Fields{"output directory": outputDirInput}).Error("Unclear where to unpack the image")
			os.Exit(1)
		}
		image, err := lib.ParseImage(imageRef)
		if err != nil {
			lib.LogE(err).WithFields(log.Fields{"input image": imageRef}).Error("Error in parsing the image")
			os.Exit(1)
		}
		lib.Log().WithFields(log.Fields{"image": imageRef, "output directory": outputDir}).Info("Unpacking the image in the output direcotry")
		err = image.UnpackFlatFilesystemInDir(outputDir)
		if err != nil {
			lib.LogE(err).WithFields(log.Fields{"input image": imageRef, "output directory": outputDir}).Error("Error in unpacking the image in the output directory")
			os.Exit(1)
		}
	},
}
