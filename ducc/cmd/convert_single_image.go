package cmd

import (
	"os"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	"github.com/cvmfs/ducc/lib"
)

func init() {
	convertSingleImageCmd.Flags().BoolVarP(&skipFlat, "skip-flat", "s", false, "do not create a flat images (compatible with singularity)")
	convertSingleImageCmd.Flags().BoolVarP(&skipLayers, "skip-layers", "d", false, "do not unpack the layers into the repository")
	rootCmd.AddCommand(convertSingleImageCmd)
}

var convertSingleImageCmd = &cobra.Command{
	Use:   "convert-single-image [image to convert] [cvmfs repository]",
	Short: "Convert a single image",
	Args:  cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		AliveMessage()

		inputImage := args[0]
		cvmfsRepo := args[1]
		if !lib.RepositoryExists(cvmfsRepo) {
			lib.Log().Error("The repository does not seems to exists.")
			os.Exit(RepoNotExistsError)
		}

		wish := lib.WishFriendly{CvmfsRepo: cvmfsRepo, InputName: inputImage}
		fields := log.Fields{"input image": wish.InputName,
			"repository": wish.CvmfsRepo}
		if !skipFlat {
			err := lib.ConvertWishSingularity(wish)
			if err != nil {
				lib.LogE(err).WithFields(fields).Error("Error in converting singularity image")
			}
		}
		if !skipLayers {
			err := lib.ConvertWishDocker(wish, convertAgain, overwriteLayer)
			if err != nil {
				lib.LogE(err).WithFields(fields).Error("Error in converting wish (docker), going on")
			}
		}
	},
}
