package cmd

import (
	"io/ioutil"
	"os"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	cvmfs "github.com/cvmfs/ducc/cvmfs"
	exec "github.com/cvmfs/ducc/exec"
	"github.com/cvmfs/ducc/lib"
	l "github.com/cvmfs/ducc/log"
)

// errors
var (
	NoPasswordError      = 101
	GetRecipeFileError   = 102
	ParseRecipeFileError = 103
	RepoNotExistsError   = 104
)

var (
	convertAgain, overwriteLayer, skipLayers, skipFlat, skipThinImage, skipPodman bool
)

func init() {
	convertCmd.Flags().BoolVarP(&overwriteLayer, "overwrite-layers", "f", false, "overwrite the layer if they are already inside the CVMFS repository")
	convertCmd.Flags().BoolVarP(&convertAgain, "convert-again", "g", false, "convert again images that are already successfull converted")
	convertCmd.Flags().BoolVarP(&skipFlat, "skip-flat", "s", false, "do not create a flat image (compatible with singularity)")
	convertCmd.Flags().BoolVarP(&skipLayers, "skip-layers", "d", false, "do not unpack the layers into the repository, implies --skip-thin-image and --skip-podman")
	convertCmd.Flags().BoolVarP(&skipThinImage, "skip-thin-image", "i", false, "do not create and push the docker thin image")
	convertCmd.Flags().BoolVarP(&skipPodman, "skip-podman", "p", false, "do not create podman image store")
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
				l.LogE(err).Error("No password provide to upload the docker images")
				os.Exit(NoPasswordError)
			}
		}

		defer exec.ExecCommand("docker", "system", "prune", "--force", "--all")

		data, err := ioutil.ReadFile(args[0])
		if err != nil {
			l.LogE(err).Error("Impossible to read the recipe file")
			os.Exit(GetRecipeFileError)
		}
		recipe, err := lib.ParseYamlRecipeV1(data)
		if err != nil {
			l.LogE(err).Error("Impossible to parse the recipe file")
			os.Exit(ParseRecipeFileError)
		}
		if !cvmfs.RepositoryExists(recipe.Repo) {
			l.LogE(err).Error("The repository does not seems to exists.")
			os.Exit(RepoNotExistsError)
		}
		for wish := range recipe.Wishes {
			fields := log.Fields{"input image": wish.InputName,
				"repository":   wish.CvmfsRepo,
				"output image": wish.OutputName}
			l.Log().WithFields(fields).Info("Start conversion of wish")
			if !skipLayers {
				err = lib.ConvertWish(wish, convertAgain, overwriteLayer)
				if err != nil {
					l.LogE(err).WithFields(fields).Error("Error in converting wish (layers), going on")
				}
			}
			if !skipThinImage {
				err = lib.ConvertWishDocker(wish)
				if err != nil {
					l.LogE(err).WithFields(fields).Error("Error in converting wish (docker), going on")
				}
			}
			if !skipPodman {
				err = lib.ConvertWishPodman(wish, convertAgain)
				if err != nil {
					l.LogE(err).WithFields(fields).Error("Error in converting wish (podman), going on")
				}
			}
			if !skipFlat {
				err = lib.ConvertWishFlat(wish)
				if err != nil {
					l.LogE(err).WithFields(fields).Error("Error in converting wish (singularity), going on")
				}
			}
		}
	},
}
