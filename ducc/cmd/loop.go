package cmd

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/signal"
	"strings"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	cvmfs "github.com/cvmfs/ducc/cvmfs"
	exec "github.com/cvmfs/ducc/exec"
	"github.com/cvmfs/ducc/lib"
	l "github.com/cvmfs/ducc/log"
)

func init() {
	loopCmd.Flags().BoolVarP(&overwriteLayer, "overwrite-layers", "f", false, "overwrite the layer if they are already inside the CVMFS repository")
	loopCmd.Flags().BoolVarP(&convertAgain, "convert-again", "g", false, "convert again images that are already successfully converted")
	loopCmd.Flags().BoolVarP(&skipFlat, "skip-flat", "s", false, "do not create a flat images (compatible with singularity)")
	loopCmd.Flags().BoolVarP(&skipLayers, "skip-layers", "d", false, "[DEPRECATED] this option is no longer functional, layers will be unpacked regardless. Use `docker save` and `cvmfs_server ingest` if you only need the flat image.")
	loopCmd.Flags().BoolVarP(&skipThinImage, "skip-thin-image", "i", false, "do not create and push the docker thin image")
	loopCmd.Flags().BoolVarP(&skipPodman, "skip-podman", "p", false, "do not create podman image store")
	loopCmd.Flags().IntVar(&maxConcurrentDownloads, "max-concurrent-downloads", 0, "maximum number of layer downloads in parallel (0 means unlimited)")
	rootCmd.AddCommand(loopCmd)
}

var loopCmd = &cobra.Command{
	Use:   "loop <wish-list.yaml>",
	Short: "An infinite loop that keep converting all the images",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		AliveMessage()
		defer exec.ExecCommand("docker", "system", "prune", "--force", "--all")
		showWeReceivedSignal := make(chan os.Signal, 1)
		signal.Notify(showWeReceivedSignal, os.Interrupt)

		stopWishLoopSignal := make(chan os.Signal, 1)
		signal.Notify(stopWishLoopSignal, os.Interrupt)

		go func() {
			<-showWeReceivedSignal
			l.Log().Info("Received SIGINT (Ctrl-C) waiting the last layer to upload then exiting.")
		}()

		checkQuitSignal := func() {
			select {
			case <-stopWishLoopSignal:
				l.Log().Info("Received SIGINT (Ctrl-C) Quitting")
				os.Exit(1)
			default:
			}
		}

		if skipLayers {
			l.Log().Warn("--skip-layers is deprecated and no longer functional: layers will be unpacked regardless. If you only need the flat image, use `docker save` and `cvmfs_server ingest` instead.")
		}

		for {
			data, err := ioutil.ReadFile(args[0])
			if err != nil {
				l.LogE(err).Fatal("Impossible to read the recipe file")
				os.Exit(1)
			}
			recipe, err := lib.ParseYamlRecipeV1(data)
			if err != nil {
				l.LogE(err).Fatal("Impossible to parse the recipe file")
				os.Exit(1)
			}
			if !cvmfs.RepositoryExists(recipe.Repo) {
				l.LogE(err).Error("The repository does not exists.")
				os.Exit(RepoNotExistsError)
			}
			var conversionErrors []string
			for wish := range recipe.Wishes {
				fields := log.Fields{"input image": wish.InputName,
					"repository":   wish.CvmfsRepo,
					"output image": wish.OutputName}
				l.Log().WithFields(fields).Info("Start conversion of wish")

				// Check if this wish is in the ignore errors list
				isIgnored := isInIgnoreList(wish.InputName, recipe.IgnoreErrorsList)

				err = lib.ConvertWish(wish, convertAgain, overwriteLayer, maxConcurrentDownloads)
				if err != nil {
					if isIgnored {
						l.LogE(err).WithFields(fields).Warning("Error in converting wish (layers), but image is in ignoreErrors list")
					} else {
						l.LogE(err).WithFields(fields).Error("Error in converting wish (layers), going on")
						conversionErrors = append(conversionErrors, fmt.Sprintf("[%s] layers: %s", wish.InputName, err))
					}
				}
				if !skipThinImage {
					err = lib.ConvertWishDocker(wish)
					if err != nil {
						if isIgnored {
							l.LogE(err).WithFields(fields).Warning("Error in converting wish (docker), but image is in ignoreErrors list")
						} else {
							l.LogE(err).WithFields(fields).Error("Error in converting wish (docker), going on")
							conversionErrors = append(conversionErrors, fmt.Sprintf("[%s] docker: %s", wish.InputName, err))
						}
					}
				}
				if !skipPodman {
					err = lib.ConvertWishPodman(wish, convertAgain)
					if err != nil {
						if isIgnored {
							l.LogE(err).WithFields(fields).Warning("Error in converting wish (podman), but image is in ignoreErrors list")
						} else {
							l.LogE(err).WithFields(fields).Error("Error in converting wish (podman), going on")
							conversionErrors = append(conversionErrors, fmt.Sprintf("[%s] podman: %s", wish.InputName, err))
						}
					}
				}
				if !skipFlat {
					err = lib.ConvertWishFlat(wish)
					if err != nil {
						if isIgnored {
							l.LogE(err).WithFields(fields).Warning("Error in converting wish (singularity), but image is in ignoreErrors list")
						} else {
							l.LogE(err).WithFields(fields).Error("Error in converting wish (singularity), going on")
							conversionErrors = append(conversionErrors, fmt.Sprintf("[%s] singularity: %s", wish.InputName, err))
						}
					}
				}
				checkQuitSignal()
			}
			if len(conversionErrors) > 0 {
				summary := fmt.Sprintf("%d conversion error(s) in this iteration:\n  %s", len(conversionErrors), strings.Join(conversionErrors, "\n  "))
				l.Log().Error(summary)
			}
			checkQuitSignal()
		}
	},
}
