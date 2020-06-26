package cmd

import (
	"os"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	"github.com/cvmfs/ducc/lib"
)

var (
	thinImageName string
)

func init() {
	convertSingleImageCmd.Flags().BoolVarP(&skipFlat, "skip-flat", "s", false, "do not create a flat images (compatible with singularity)")
	convertSingleImageCmd.Flags().BoolVarP(&skipLayers, "skip-layers", "d", false, "do not unpack the layers into the repository, implies --skip-thin-image")
	convertSingleImageCmd.Flags().BoolVarP(&skipThinImage, "skip-thin-image", "i", false, "do not create and push the docker thin image")
	convertSingleImageCmd.Flags().StringVarP(&username, "username", "u", "", "username to use when pushing thin image into the docker registry")
	convertSingleImageCmd.Flags().StringVarP(&thinImageName, "thin-image-name", "", "", "name to use for the thin image to upload, if empty implies --skip-thin-image.")
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

		if skipLayers == true {
			lib.Log().Info("Skipping the creation of the thin image since provided --skip-layers")
			skipThinImage = true
		}
		if thinImageName == "" {
			lib.Log().Info("Skipping the creation of the thin image since did not provided a name for the thin image using the --thin-image-name flag")
			skipThinImage = true
			// we need a thinImageName to parse the wish
			thinImageName = inputImage + "_thin"
		}

		if skipThinImage == false {
			_, err := lib.GetPassword()
			if err != nil {
				lib.LogE(err).Warning("Asked to create the docker thin image but did not provide the password for the registry, we cannot push the thin image to the registry, hence we won't create it. We will unpack the layers.")
				skipThinImage = true
			}
		}

		if !lib.RepositoryExists(cvmfsRepo) {
			lib.Log().Error("The repository does not seems to exists.")
			os.Exit(RepoNotExistsError)
		}

		wish := lib.WishFriendly{
			CvmfsRepo:  cvmfsRepo,
			InputName:  inputImage,
			UserOutput: username,
			OutputName: thinImageName}
		fields := log.Fields{"input image": wish.InputName,
			"repository": wish.CvmfsRepo}
		if !skipFlat {
			err := lib.ConvertWishSingularity(wish)
			if err != nil {
				lib.LogE(err).WithFields(fields).Error("Error in converting singularity image")
			}
		}
		if !skipLayers {
			err := lib.ConvertWishDocker(wish, convertAgain, overwriteLayer, !skipThinImage)
			if err != nil {
				lib.LogE(err).WithFields(fields).Error("Error in converting wish (docker), going on")
			}
		}
	},
}
