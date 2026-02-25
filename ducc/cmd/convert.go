package cmd

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	cvmfs "github.com/cvmfs/ducc/cvmfs"
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
	convertAgain, overwriteLayer, skipLayers, skipFlat, skipThinImage, skipPodman, multiArch bool
	maxConcurrentDownloads                                                                    int
)

func init() {
	convertCmd.Flags().BoolVarP(&overwriteLayer, "overwrite-layers", "f", false, "overwrite the layer if they are already inside the CVMFS repository")
	convertCmd.Flags().BoolVarP(&convertAgain, "convert-again", "g", false, "convert again images that are already successfully converted")
	convertCmd.Flags().BoolVarP(&skipFlat, "skip-flat", "s", false, "do not create a flat image (compatible with singularity)")
	convertCmd.Flags().BoolVarP(&skipLayers, "skip-layers", "d", false, "[DEPRECATED] this option is no longer functional, layers will be unpacked regardless. Use `docker save` and `cvmfs_server ingest` if you only need the flat image.")
	convertCmd.Flags().BoolVarP(&skipThinImage, "skip-thin-image", "i", false, "do not create and push the docker thin image")
	convertCmd.Flags().BoolVarP(&skipPodman, "skip-podman", "p", false, "do not create podman image store")
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
		var conversionErrors []string
		for wish := range recipe.Wishes {
			fields := log.Fields{"input image": wish.InputName,
				"repository":   wish.CvmfsRepo,
				"output image": wish.OutputName}
			l.Log().WithFields(fields).Info("Start conversion of wish")

			// Check if this wish is in the ignore errors list
			isIgnored := isInIgnoreList(wish.InputName, recipe.IgnoreErrorsList)

			err = lib.ConvertWish(wish, convertAgain, overwriteLayer, multiArch, maxConcurrentDownloads)
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
				err = lib.ConvertWishFlat(wish, multiArch)
				if err != nil {
					if isIgnored {
						l.LogE(err).WithFields(fields).Warning("Error in converting wish (singularity), but image is in ignoreErrors list")
					} else {
						l.LogE(err).WithFields(fields).Error("Error in converting wish (singularity), going on")
						conversionErrors = append(conversionErrors, fmt.Sprintf("[%s] singularity: %s", wish.InputName, err))
					}
				}
			}
		}
		if len(conversionErrors) > 0 {
			summary := fmt.Sprintf("%d conversion error(s):\n  %s", len(conversionErrors), strings.Join(conversionErrors, "\n  "))
			l.Log().Error(summary)
			return fmt.Errorf("%s", summary)
		}
		return nil
	},
}
