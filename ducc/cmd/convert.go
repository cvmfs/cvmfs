package cmd

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

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
	convertAgain, overwriteLayer, skipLayers, skipFlat, skipPodman, skipThinImage, multiArch bool
	maxConcurrentDownloads                                                                   int
)

func init() {
	convertCmd.Flags().BoolVarP(&overwriteLayer, "overwrite-layers", "f", false, "overwrite the layer if they are already inside the CVMFS repository")
	convertCmd.Flags().BoolVarP(&convertAgain, "convert-again", "g", false, "convert again images that are already successfully converted")
	convertCmd.Flags().BoolVarP(&skipFlat, "skip-flat", "s", false, "do not create a flat image (compatible with singularity)")
	convertCmd.Flags().BoolVarP(&skipPodman, "skip-podman", "p", false, "do not publish to the podman additional image store at podmanStore/")
	convertCmd.Flags().BoolVarP(&skipLayers, "skip-layers", "d", false, "[DEPRECATED] this option is no longer functional, layers will be unpacked regardless. Use `docker save` and `cvmfs_server ingest` if you only need the flat image.")
	convertCmd.Flags().BoolVarP(&skipThinImage, "skip-thin-image", "i", false, "do not create and push the docker thin image")
	convertCmd.Flags().BoolVarP(&multiArch, "multi-arch", "m", false, "convert all architectures for multi-arch images")
	convertCmd.Flags().IntVar(&maxConcurrentDownloads, "max-concurrent-downloads", 0, "maximum number of layer downloads in parallel (0 means unlimited, env: DUCC_MAX_CONCURRENT_DOWNLOADS)")
	rootCmd.AddCommand(convertCmd)
}

// applyMaxConcurrentDownloadsEnv sets maxConcurrentDownloads from the
// DUCC_MAX_CONCURRENT_DOWNLOADS environment variable when the CLI flag was not
// explicitly provided.
func applyMaxConcurrentDownloadsEnv(cmd *cobra.Command) {
	if !cmd.Flags().Changed("max-concurrent-downloads") {
		if envVal := os.Getenv("DUCC_MAX_CONCURRENT_DOWNLOADS"); envVal != "" {
			if v, err := strconv.Atoi(envVal); err == nil {
				maxConcurrentDownloads = v
			} else {
				l.Log().WithField("value", envVal).Warn("Invalid DUCC_MAX_CONCURRENT_DOWNLOADS value, ignoring")
			}
		}
	}
}

// isInIgnoreList checks if an image name matches any pattern in the ignore list
func isInIgnoreList(imageName string, ignoreList []string) bool {
	for _, pattern := range ignoreList {
		matched, err := filepath.Match(pattern, imageName)
		if err != nil {
			l.LogE(err).WithFields(log.Fields{"pattern": pattern}).Warning("Invalid ignore pattern")
			continue
		}
		if matched {
			return true
		}
	}
	return false
}

var convertCmd = &cobra.Command{
	Use:   "convert <wish-list.yaml>",
	Short: "Convert the wishes",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) (err error) {
		AliveMessage()
		applyMaxConcurrentDownloadsEnv(cmd)

		if skipLayers {
			l.Log().Warn("--skip-layers is deprecated and no longer functional: layers will be unpacked regardless. If you only need the flat image, use `docker save` and `cvmfs_server ingest` instead.")
		}

		if (skipLayers == false) && (skipThinImage == false) {
			_, err := lib.GetPassword()
			if err != nil {
				l.LogE(err).Error("No password provide to upload the docker images")
				return (err)
			}
		}

		data, err := ioutil.ReadFile(args[0])
		if err != nil {
			l.LogE(err).Error("Impossible to read the recipe file")
			return err
		}
		recipe, err := lib.ParseYamlRecipeV1(data)
		if err != nil {
			l.LogE(err).Error("Impossible to parse the recipe file")
			return err
		}
		if !cvmfs.RepositoryExists(recipe.Repo) {
			l.LogE(err).Error("The repository does not seem to exists.")
			return err
		}

		// Set up signal handler to abort the cvmfs transaction on Ctrl-C
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
		go func() {
			sig := <-sigChan
			repoName, _ := cvmfs.GetRepoAndSubdir(recipe.Repo)
			l.Log().WithFields(log.Fields{"signal": sig, "repo": repoName}).
				Info("Received signal, trying to abort the cvmfs transaction")
			exec.ExecCommand("cvmfs_server", "abort", "-f", repoName).Start()
			os.Exit(1)
		}()
		defer signal.Stop(sigChan)
		if !skipThinImage && recipe.OutputFormat == "" {
			l.Log().Info("Using default output image name $(scheme)://$(registry)/$(repository)_thin:$(tag)")
		}

		var conversionErrors []string
		totalSummary := lib.ConversionSummary{}
		for wish := range recipe.Wishes {
			fields := log.Fields{"input image": wish.InputName,
				"repository":   wish.CvmfsRepo,
				"output image": wish.OutputName}
			l.Log().WithFields(fields).Info("Start conversion of wish")

			// Check if this wish is in the ignore errors list
			isIgnored := isInIgnoreList(wish.InputName, recipe.IgnoreErrorsList)

			summary, layerErr := lib.ConvertWish(wish, convertAgain, overwriteLayer, multiArch, maxConcurrentDownloads)
			totalSummary.Merge(summary)
			if layerErr != nil {
				if isIgnored {
					l.LogE(layerErr).WithFields(fields).Warning("Error in converting wish (layers), but image is in ignoreErrors list")
				} else {
					l.LogE(layerErr).WithFields(fields).Error("Error in converting wish (layers), going on")
					conversionErrors = append(conversionErrors, fmt.Sprintf("[%s] layers: %s", wish.InputName, layerErr))
				}
			} else {
				if len(summary.Added) == 0 && len(summary.Updated) == 0 {
					l.Log().WithFields(fields).Info("All layers already converted, nothing to do")
				} else {
					l.Log().WithFields(fields).Info("Successfully converted the layers")
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
			if !skipFlat {
				if layerErr != nil {
					l.LogE(layerErr).WithFields(fields).Warning("Skipping overlay: layer ingestion had errors")
				} else {
					err = lib.ConvertWishFlat(wish, multiArch)
					if err != nil {
						if isIgnored {
							l.LogE(err).WithFields(fields).Warning("Error in converting wish (singularity), but image is in ignoreErrors list")
						} else {
							l.LogE(err).WithFields(fields).Error("Error in converting wish (singularity), going on")
							conversionErrors = append(conversionErrors, fmt.Sprintf("[%s] singularity: %s", wish.InputName, err))
						}
					} else if !skipPodman {
						if podmanErr := lib.ConvertWishPodman(wish, multiArch); podmanErr != nil {
							if isIgnored {
								l.LogE(podmanErr).WithFields(fields).Warning("Error publishing podman store, but image is in ignoreErrors list")
							} else {
								l.LogE(podmanErr).WithFields(fields).Error("Error publishing podman store, going on")
								conversionErrors = append(conversionErrors, fmt.Sprintf("[%s] podman: %s", wish.InputName, podmanErr))
							}
						}
					}
				}
			}
		}
		logConversionSummary("Conversion summary:", totalSummary)
		if len(conversionErrors) > 0 {
			summary := fmt.Sprintf("%d conversion error(s):\n  %s", len(conversionErrors), strings.Join(conversionErrors, "\n  "))
			l.Log().Error(summary)
			return fmt.Errorf("%s", summary)
		}
		return nil
	},
}
