package cmd

import (
	"os"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	"github.com/cvmfs/ducc/cvmfs"
	"github.com/cvmfs/ducc/lib"
	l "github.com/cvmfs/ducc/log"
)

var (
	thinImageName string
	attempts      int
)

func init() {
	convertSingleImageCmd.Flags().BoolVarP(&skipFlat, "skip-flat", "s", false, "do not create a flat images (compatible with singularity)")
	convertSingleImageCmd.Flags().BoolVarP(&skipLayers, "skip-layers", "d", false, "do not unpack the layers into the repository, implies --skip-thin-image and --skip-podman")
	convertSingleImageCmd.Flags().BoolVarP(&skipThinImage, "skip-thin-image", "i", false, "do not create and push the docker thin image")
	convertSingleImageCmd.Flags().BoolVarP(&skipPodman, "skip-podman", "p", false, "do not create podman image store")
	convertSingleImageCmd.Flags().StringVarP(&username, "username", "u", "", "username to use when pushing thin image into the docker registry")
	convertSingleImageCmd.Flags().StringVarP(&thinImageName, "thin-image-name", "", "", "name to use for the thin image to upload, if empty implies --skip-thin-image.")
	convertSingleImageCmd.Flags().IntVarP(&attempts, "attempts", "r", 1, "number of time to try to unpack the image, default one")
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
			l.Log().Info("Skipping the creation of the thin image and podman store since provided --skip-layers")
			skipThinImage = true
			skipPodman = true
		}
		if thinImageName == "" {
			l.Log().Info("Skipping the creation of the thin image since did not provided a name for the thin image using the --thin-image-name flag")
			skipThinImage = true
			// we need a thinImageName to parse the wish
			thinImageName = inputImage + "_thin"
		}

		if skipThinImage == false {
			_, err := lib.GetPassword()
			if err != nil {
				l.LogE(err).Warning("Asked to create the docker thin image but did not provide the password for the registry, we cannot push the thin image to the registry, hence we won't create it. We will unpack the layers.")
				skipThinImage = true
			}
		}

		if !cvmfs.RepositoryExists(cvmfsRepo) {
			l.Log().Error("The repository does not seems to exists.")
			os.Exit(RepoNotExistsError)
		}

		wish, err := lib.CreateWish(inputImage, thinImageName, cvmfsRepo, username, username)
		if err != nil {
			l.LogE(err).Error("Error in creating the wish to convert")
			os.Exit(1)
		}
		fields := log.Fields{
			"input image":    wish.InputName,
			"repository":     wish.CvmfsRepo,
			"total attempts": attempts}

		if !skipFlat {
			for i := 0; i < attempts; i++ {
				err := lib.ConvertWishFlat(wish)
				log := l.LogE(err).WithFields(fields).
					WithFields(log.Fields{"attempts number": i})
				if err != nil {
					log.Error("Error in converting singularity image")
				} else {
					log.Info("Successfully created the singularity image")
					break
				}
			}
		}

		if !skipLayers {
			for i := 0; i < attempts; i++ {
				err := lib.ConvertWish(wish, convertAgain, overwriteLayer)
				log := l.LogE(err).WithFields(fields).
					WithFields(log.Fields{"attempts number": i})
				if err != nil {
					log.Error("Error in converting wish (layers)")
				} else {
					log.Info("Successfully converted the layers")
					break
				}
			}
		}

		if !skipThinImage {
			for i := 0; i < attempts; i++ {
				err := lib.ConvertWishDocker(wish)
				log := l.LogE(err).WithFields(fields).
					WithFields(log.Fields{"attempts number": i})
				if err != nil {
					log.Error("Error in converting wish (docker)")
				} else {
					log.Info("Successfully converted wish (docker)")
					break
				}
			}
		}

		if !skipPodman {
			for i := 0; i < attempts; i++ {
				err := lib.ConvertWishPodman(wish, convertAgain)
				log := l.LogE(err).WithFields(fields).
					WithFields(log.Fields{"attempts number": i})
				if err != nil {
					log.Error("Error in converting wish (podman)")
				} else {
					log.Info("Successfully converted with (podman)")
					break
				}
			}
		}
	},
}
