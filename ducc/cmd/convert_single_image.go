package cmd

import (
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	"github.com/cvmfs/ducc/cvmfs"
	exec "github.com/cvmfs/ducc/exec"
	"github.com/cvmfs/ducc/lib"
	l "github.com/cvmfs/ducc/log"
)

var (
	thinImageName string
	attempts      int
)

func init() {
	convertSingleImageCmd.Flags().BoolVarP(&skipFlat, "skip-flat", "s", false, "do not create a flat images (compatible with singularity)")
	convertSingleImageCmd.Flags().BoolVarP(&skipLayers, "skip-layers", "d", false, "[DEPRECATED] this option is no longer functional, layers will be unpacked regardless. Use `docker save` and `cvmfs_server ingest` if you only need the flat image.")
	convertSingleImageCmd.Flags().BoolVarP(&skipThinImage, "skip-thin-image", "i", true, "do not create and push the docker thin image")
	convertSingleImageCmd.Flags().BoolVarP(&skipPodman, "skip-podman", "p", true, "do not create podman image store")
	convertSingleImageCmd.Flags().StringVarP(&username, "username", "u", "", "username to use when pushing thin image into the docker registry")
	convertSingleImageCmd.Flags().StringVarP(&thinImageName, "thin-image-name", "", "", "name to use for the thin image to upload, if empty implies --skip-thin-image.")
	convertSingleImageCmd.Flags().IntVarP(&attempts, "attempts", "r", 1, "number of time to try to unpack the image, default one")
	convertSingleImageCmd.Flags().BoolVarP(&multiArch, "multi-arch", "m", false, "Convert all architectures for multi-arch images")
	convertSingleImageCmd.Flags().IntVar(&maxConcurrentDownloads, "max-concurrent-downloads", 0, "maximum number of layer downloads in parallel (0 means unlimited, env: DUCC_MAX_CONCURRENT_DOWNLOADS)")
	rootCmd.AddCommand(convertSingleImageCmd)
}

var convertSingleImageCmd = &cobra.Command{
	Use:   "convert-single-image <image to convert> <cvmfs repository>",
	Short: "Convert a single image",
	Args:  cobra.ExactArgs(2),
	RunE: func(cmd *cobra.Command, args []string) (err error) {
		AliveMessage()
		applyMaxConcurrentDownloadsEnv(cmd)

		inputImage := args[0]
		cvmfsRepo := args[1]

		if skipLayers {
			l.Log().Warn("--skip-layers is deprecated and no longer functional: layers will be unpacked regardless. If you only need the flat image, use `docker save` and `cvmfs_server ingest` instead.")
			skipThinImage = true
			skipPodman = true
		}
		if thinImageName == "" {
			if !skipThinImage {
				l.Log().Trace("Skipping the creation of the thin image since no name was provided via --thin-image-name")
				skipThinImage = true
			}
			// we need a thinImageName to parse the wish
			thinImageName = inputImage + "_thin"
		}

		if skipThinImage == false {
			_, err = lib.GetPassword()
			if err != nil {
				l.LogE(err).Warning("Asked to create the docker thin image but did not provide the password for the registry, we cannot push the thin image to the registry, hence we won't create it.")
				skipThinImage = true
				return err
			}
		}

		if !cvmfs.RepositoryExists(cvmfsRepo) {
			l.Log().Errorf("The repository %s does not seem to exist.", cvmfsRepo)
			return fmt.Errorf("The repository %s does not seem to exist.", cvmfsRepo)
		}

		// Set up signal handler to abort the cvmfs transaction on Ctrl-C
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
		go func() {
			sig := <-sigChan
			repoName, _ := cvmfs.GetRepoAndSubdir(cvmfsRepo)
			l.Log().WithFields(log.Fields{"signal": sig, "repo": repoName}).
				Info("Received signal, trying to abort the cvmfs transaction")
			exec.ExecCommand("cvmfs_server", "abort", "-f", repoName).Start()
			os.Exit(1)
		}()
		defer signal.Stop(sigChan)

		input, err := lib.ParseImage(inputImage)
		wish, err := lib.CreateWish(input, thinImageName, cvmfsRepo, username, username)
		if err != nil {
			l.LogE(err).Error("Error in creating the wish to convert")
			return err
		}
		fields := log.Fields{
			"input image":    wish.InputName,
			"repository":     wish.CvmfsRepo,
			"total attempts": attempts}

		var conversionErrors []string
		totalSummary := lib.ConversionSummary{}

		for i := 0; i < attempts; i++ {
			attemptSummary, err := lib.ConvertWish(wish, convertAgain, overwriteLayer, multiArch, maxConcurrentDownloads)
			totalSummary.Merge(attemptSummary)
			log := l.LogE(err).WithFields(fields).
				WithFields(log.Fields{"attempts number": i})
			if err != nil {
				log.Warning("Could not convert wish (layers), trying again")
			} else {
				if len(totalSummary.Added) == 0 && len(totalSummary.Updated) == 0 {
					log.Info("All layers already converted, nothing to do")
				} else {
					log.Info("Successfully converted the layers")
				}
				break
			}
		}
		if err != nil {
			log.Error("Multiple Errors in converting layers, going on")
			conversionErrors = append(conversionErrors, fmt.Sprintf("layers: %s", err))
		}
		logConversionSummary(fmt.Sprintf("Conversion summary for %s:", wish.InputName), totalSummary)

		if !skipFlat {
			for i := 0; i < attempts; i++ {
				err = lib.ConvertWishFlat(wish, multiArch)
				log := l.LogE(err).WithFields(fields).
					WithFields(log.Fields{"attempts number": i})
				if err != nil {
					log.Warning("Error in converting singularity image, trying again")
				} else {
					log.Info("Successfully created the singularity image")
					break
				}
			}

			if err != nil {
				log.Error("Multiple Errors in converting singularity image, going on")
				conversionErrors = append(conversionErrors, fmt.Sprintf("singularity: %s", err))
			}
		}

		if !skipThinImage {
			for i := 0; i < attempts; i++ {
				err = lib.ConvertWishDocker(wish)
				log := l.LogE(err).WithFields(fields).
					WithFields(log.Fields{"attempts number": i})
				if err != nil {
					log.Warning("Could not convert  wish (docker), trying again")
				} else {
					log.Info("Successfully converted wish (docker)")
					break
				}
			}
			if err != nil {
				log.Error("Multiple Errors in converting wish (docker), going on")
				conversionErrors = append(conversionErrors, fmt.Sprintf("docker: %s", err))
			}
		}

		if !skipPodman {
			for i := 0; i < attempts; i++ {
				err = lib.ConvertWishPodman(wish, convertAgain)
				log := l.LogE(err).WithFields(fields).
					WithFields(log.Fields{"attempts number": i})
				if err != nil {
					log.Warning("Could not convert wish (podman), trying again")
				} else {
					log.Info("Successfully converted with (podman)")
					break
				}
			}
			if err != nil {
				log.Error("Multiple Errors in converting wish (podman), going on")
				conversionErrors = append(conversionErrors, fmt.Sprintf("podman: %s", err))
			}
		}
		if len(conversionErrors) > 0 {
			summary := fmt.Sprintf("%d conversion error(s) for %s:\n  %s", len(conversionErrors), wish.InputName, strings.Join(conversionErrors, "\n  "))
			l.Log().Error(summary)
			return fmt.Errorf("%s", summary)
		}
		return nil
	},
}
