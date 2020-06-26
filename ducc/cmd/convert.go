package cmd

import (
	"io/ioutil"
	"os"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	"github.com/cvmfs/ducc/lib"
)

// errors
var (
	NoPasswordError      = 101
	GetRecipeFileError   = 102
	ParseRecipeFileError = 103
	RepoNotExistsError   = 104
)

var (
	convertAgain, overwriteLayer, skipLayers, skipFlat, skipThinImage, convertPodman bool
)

func init() {
	convertCmd.Flags().BoolVarP(&overwriteLayer, "overwrite-layers", "f", false, "overwrite the layer if they are already inside the CVMFS repository")
	convertCmd.Flags().BoolVarP(&convertAgain, "convert-again", "g", false, "convert again images that are already successfull converted")
	convertCmd.Flags().BoolVarP(&skipFlat, "skip-flat", "s", false, "do not create a flat image (compatible with singularity)")
	convertCmd.Flags().BoolVarP(&skipLayers, "skip-layers", "d", false, "do not unpack the layers into the repository, implies --skip-thin-image")
	convertCmd.Flags().BoolVarP(&skipThinImage, "skip-thin-image", "i", false, "do not create and push the docker thin image")
	convertCmd.Flags().BoolVarP(&convertPodman, "convert-podman", "p", true, "also create a podman image store")
	rootCmd.AddCommand(convertCmd)
}

var convertCmd = &cobra.Command{
	Use:   "convert wish-list.yaml",
	Short: "Convert the wishes",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		AliveMessage()

		if (skipLayers == false) && (skipThinImage == false) {
			_, err := lib.GetPassword()
			if err != nil {
				lib.LogE(err).Error("No password provide to upload the docker images")
				os.Exit(NoPasswordError)
			}
		}

		defer lib.ExecCommand("docker", "system", "prune", "--force", "--all")

		data, err := ioutil.ReadFile(args[0])
		if err != nil {
			lib.LogE(err).Error("Impossible to read the recipe file")
			os.Exit(GetRecipeFileError)
		}
		recipe, err := lib.ParseYamlRecipeV1(data)
		if err != nil {
			lib.LogE(err).Error("Impossible to parse the recipe file")
			os.Exit(ParseRecipeFileError)
		}
		if !lib.RepositoryExists(recipe.Repo) {
			lib.LogE(err).Error("The repository does not seems to exists.")
			os.Exit(RepoNotExistsError)
		}
		for wish := range recipe.Wishes {
			fields := log.Fields{"input image": wish.InputName,
				"repository":   wish.CvmfsRepo,
				"output image": wish.OutputName}
			lib.Log().WithFields(fields).Info("Start conversion of wish")
			if !skipLayers {
				err = lib.ConvertWishDocker(wish, convertAgain, overwriteLayer, !skipThinImage, convertPodman)
				if err != nil {
					lib.LogE(err).WithFields(fields).Error("Error in converting wish (docker), going on")
				}
			}
			if !skipFlat {
				err = lib.ConvertWishSingularity(wish)
				if err != nil {
					lib.LogE(err).WithFields(fields).Error("Error in converting wish (singularity), going on")
				}
			}
			if convertPodman {
				err = lib.ConvertWishPodman(wish, skipLayers)
				if err != nil {
					lib.LogE(err).WithFields(fields).Error("Error in converting wish (podman), going on")
				}
			}
		}
	},
}
