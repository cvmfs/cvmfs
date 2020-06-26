package cmd

import (
	"io/ioutil"
	"os"
	"os/signal"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	"github.com/cvmfs/ducc/lib"
)

func init() {
	loopCmd.Flags().BoolVarP(&overwriteLayer, "overwrite-layers", "f", false, "overwrite the layer if they are already inside the CVMFS repository")
	loopCmd.Flags().BoolVarP(&convertAgain, "convert-again", "g", false, "convert again images that are already successfull converted")
	loopCmd.Flags().BoolVarP(&skipFlat, "skip-flat", "s", false, "do not create a flat images (compatible with singularity)")
	loopCmd.Flags().BoolVarP(&skipLayers, "skip-layers", "d", false, "do not unpack the layers into the repository, implies --skip-thin-image")
	loopCmd.Flags().BoolVarP(&skipThinImage, "skip-thin-image", "i", false, "do not create and push the docker thin image")
	rootCmd.AddCommand(loopCmd)
}

var loopCmd = &cobra.Command{
	Use:   "loop wish-list.yaml",
	Short: "An infinite loop that keep converting all the images",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		AliveMessage()
		defer lib.ExecCommand("docker", "system", "prune", "--force", "--all")
		showWeReceivedSignal := make(chan os.Signal, 1)
		signal.Notify(showWeReceivedSignal, os.Interrupt)

		stopWishLoopSignal := make(chan os.Signal, 1)
		signal.Notify(stopWishLoopSignal, os.Interrupt)

		go func() {
			<-showWeReceivedSignal
			lib.Log().Info("Received SIGINT (Ctrl-C) waiting the last layer to upload then exiting.")
		}()

		checkQuitSignal := func() {
			select {
			case <-stopWishLoopSignal:
				lib.Log().Info("Received SIGINT (Ctrl-C) Quitting")
				os.Exit(1)
			default:
			}
		}

		for {
			data, err := ioutil.ReadFile(args[0])
			if err != nil {
				lib.LogE(err).Fatal("Impossible to read the recipe file")
				os.Exit(1)
			}
			recipe, err := lib.ParseYamlRecipeV1(data)
			if err != nil {
				lib.LogE(err).Fatal("Impossible to parse the recipe file")
				os.Exit(1)
			}
			if !lib.RepositoryExists(recipe.Repo) {
				lib.LogE(err).Error("The repository does not exists.")
				os.Exit(RepoNotExistsError)
			}
			for wish := range recipe.Wishes {
				fields := log.Fields{"input image": wish.InputName,
					"repository":   wish.CvmfsRepo,
					"output image": wish.OutputName}
				lib.Log().WithFields(fields).Info("Start conversion of wish")
				if !skipLayers {
					err = lib.ConvertWishDocker(wish, convertAgain, overwriteLayer, !skipThinImage)
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
				checkQuitSignal()
			}
			checkQuitSignal()
		}
	},
}
