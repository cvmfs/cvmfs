package cmd

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"sync"
	"sync/atomic"

	"github.com/spf13/cobra"

	"github.com/cvmfs/ducc/config"
	"github.com/cvmfs/ducc/cvmfs"
	"github.com/cvmfs/ducc/daemon"
	"github.com/cvmfs/ducc/db"
	"github.com/cvmfs/ducc/errorcodes"
	l "github.com/cvmfs/ducc/log"
	"github.com/cvmfs/ducc/products"
	"github.com/cvmfs/ducc/registry"
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
	Use:        "convert-single-image [image to convert] [cvmfs repository]",
	Short:      "Convert a single image",
	Args:       cobra.ExactArgs(2),
	Deprecated: "please use 'wish add --standalone' instead. ",
	Run: func(cmd *cobra.Command, args []string) {
		// Init Registries
		ctx, cancelFunc := context.WithCancel(context.Background())
		defer cancelFunc()
		registry.InitRegistries(ctx, nil, nil)

		fmt.Printf("Converting image %s\n", args[0])

		inputImage := args[0]
		cvmfsRepo := args[1]

		outputOptions := db.WishOutputOptions{
			CreateLayers:    db.ValueWithDefault[bool]{Value: config.DEFAULT_CREATELAYERS, IsDefault: true},
			CreateFlat:      db.ValueWithDefault[bool]{Value: config.DEFAULT_CREATEFLAT, IsDefault: true},
			CreatePodman:    db.ValueWithDefault[bool]{Value: config.DEFAULT_CREATEPODMAN, IsDefault: true},
			CreateThinImage: db.ValueWithDefault[bool]{Value: config.DEFAULT_CREATETHINIMAGE, IsDefault: true},
		}

		if skipLayers {
			l.Log().Info("Skipping the creation of the thin image and podman store since provided --skip-layers")
			outputOptions.CreateLayers = db.ValueWithDefault[bool]{Value: false, IsDefault: false}
			skipThinImage = true
			skipPodman = true
		}
		if skipFlat {
			l.Log().Info("Skipping the creation of the flat image since provided --skip-flat")
			outputOptions.CreateFlat = db.ValueWithDefault[bool]{Value: false, IsDefault: false}
		}
		if skipPodman {
			l.Log().Info("Skipping the creation of the podman store since provided --skip-podman")
			outputOptions.CreatePodman = db.ValueWithDefault[bool]{Value: false, IsDefault: false}
		}

		/*if thinImageName == "" {
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
		}*/

		if !cvmfs.RepositoryExists(cvmfsRepo) {
			l.Log().Error("The repository does not seems to exists.")
			os.Exit(errorcodes.RepoNotExistsError)
		}

		parsedUrl, err := daemon.ParseImageURL(inputImage)
		if err != nil {
			fmt.Printf("\"%s\" is not a valid image identifier\n", inputImage)
		}
		wish := db.Wish{
			Identifier: db.WishIdentifier{
				Wishlist:              "cli",
				CvmfsRepository:       cvmfsRepo,
				InputTag:              parsedUrl.Tag,
				InputTagWildcard:      parsedUrl.TagWildcard,
				InputDigest:           parsedUrl.Digest,
				InputRepository:       parsedUrl.Repository,
				InputRegistryScheme:   parsedUrl.Scheme,
				InputRegistryHostname: parsedUrl.Registry,
			},
		}
		images, err := registry.ExpandWildcard(wish)
		if err != nil {
			fmt.Printf("Error in expanding the wildcard string: %s\n", err)
			os.Exit(1)
		}

		if wish.Identifier.InputTagWildcard {
			if len(images) == 0 {
				fmt.Printf("The wildcard identifier \"%s\" does not match any image\n", inputImage)
				os.Exit(1)
			}
			fmt.Printf("The wildcard identifier \"%s\" matches %d image(s):\n", inputImage, len(images))
			for _, image := range images {
				fmt.Printf("\t- %s\n", image.GetSimpleName())
			}
		} else {
			fmt.Printf("Converting image %s\n", images[0].GetSimpleName())
		}
		fmt.Println("")

		// We need an in-memory database to store the tasks
		inMemDb, err := sql.Open("sqlite3", ":memory:")
		if err != nil {
			fmt.Printf("Error in creating the in-memory database: %s\n", err)
			os.Exit(1)
		}
		db.Init(inMemDb)

		totalCount := int32(len(images))
		successCount := int32(0)
		failedCount := int32(0)

		// Update each image
		wg := sync.WaitGroup{}
		for _, image := range images {
			image := image
			wg.Add(1)
			go func() {
				defer wg.Done()
				task, err := registry.FetchManifestTask(nil, image)
				if err != nil {
					fmt.Printf("Error in creating the task for image %s: %s\n", image.GetSimpleName(), err)
					os.Exit(1)
				}
				task.Start(nil)
				result := task.WaitUntilDone()
				if result != db.TASK_RESULT_SUCCESS {
					fmt.Printf("Error in fetching the manifest for image %s: %s\n", image.GetSimpleName(), result)
					atomic.AddInt32(&failedCount, 1)
					return
				}
				artifact, err := task.GetArtifact()
				if err != nil {
					fmt.Printf("Error in fetching the manifest for image %s: %s\n", image.GetSimpleName(), err)
					atomic.AddInt32(&failedCount, 1)
					return
				}
				manifest := artifact.(registry.ManifestWithBytesAndDigest)

				// Create the outputs
				updateTask, err := products.UpdateImageInRepoTask(image, manifest, outputOptions, cvmfsRepo)
				if err != nil {
					fmt.Printf("Error in creating the task for image %s: %s\n", image.GetSimpleName(), err)
					atomic.AddInt32(&failedCount, 1)
					return
				}
				updateTask.Start(nil)
				result = updateTask.WaitUntilDone()
				if result != db.TASK_RESULT_SUCCESS {
					fmt.Printf("Error in updating the image %s: %s\n", image.GetSimpleName(), result)
					atomic.AddInt32(&failedCount, 1)
					return
				}
				atomic.AddInt32(&successCount, 1)
				remaining := totalCount - atomic.LoadInt32(&successCount) - atomic.LoadInt32(&failedCount)
				if remaining < 0 {
					fmt.Printf("%d/%d image(s) remaining\n", remaining, totalCount)
				}
			}()
		}

		wg.Wait()
		fmt.Printf("Operation complete!\n")
		if successCount > 0 {
			fmt.Printf("Successfully converted %d image(s)\n", successCount)
		}
		if failedCount > 0 {
			fmt.Printf("Failed to convert %d image(s)\n", failedCount)
		}

		if failedCount > 0 {
			os.Exit(1)
		}
	},
}
