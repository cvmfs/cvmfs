package cmd

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"sync"

	"github.com/spf13/cobra"

	"github.com/cvmfs/ducc/config"
	cvmfs "github.com/cvmfs/ducc/cvmfs"
	"github.com/cvmfs/ducc/daemon"
	"github.com/cvmfs/ducc/db"
	"github.com/cvmfs/ducc/registry"
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
	rootCmd.AddCommand()
}

var convertCmd = &cobra.Command{
	Deprecated: "please use 'recipe apply --standalone' instead. ",
	Use:        "convert wish-list.yaml",
	Short:      "Convert the wishes",
	Args:       cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		// Init Registries
		ctx, cancelFunc := context.WithCancel(context.Background())
		defer cancelFunc()
		registry.InitRegistries(ctx, nil, nil)

		outputOptions := db.WishOutputOptions{
			CreateLayers:    db.ValueWithDefault[bool]{Value: config.DEFAULT_CREATELAYERS, IsDefault: true},
			CreateFlat:      db.ValueWithDefault[bool]{Value: config.DEFAULT_CREATEFLAT, IsDefault: true},
			CreatePodman:    db.ValueWithDefault[bool]{Value: config.DEFAULT_CREATEPODMAN, IsDefault: true},
			CreateThinImage: db.ValueWithDefault[bool]{Value: config.DEFAULT_CREATETHINIMAGE, IsDefault: true},
		}

		if skipLayers {
			outputOptions.CreateLayers = db.ValueWithDefault[bool]{Value: false, IsDefault: false}
			skipThinImage = true
			skipPodman = true
		}
		if skipFlat {
			outputOptions.CreateFlat = db.ValueWithDefault[bool]{Value: false, IsDefault: false}
		}
		if skipPodman {
			outputOptions.CreatePodman = db.ValueWithDefault[bool]{Value: false, IsDefault: false}
		}

		/*if (skipLayers == false) && (skipThinImage == false) {
			_, err := lib.GetPassword()
			if err != nil {
				l.LogE(err).Error("No password provide to upload the docker images")
				os.Exit(NoPasswordError)
			}
		}

		defer exec.ExecCommand("docker", "system", "prune", "--force", "--all")
		*/

		data, err := os.ReadFile(args[0])
		if err != nil {
			return fmt.Errorf("impossible to read the recipe file")
		}
		recipe, err := daemon.ParseYamlRecipeV1(data, "cmd")
		if err != nil {
			return fmt.Errorf("impossible to parse the recipe file")
		}
		if exists, err := cvmfs.RepositoryExists(recipe.CvmfsRepo); err != nil {
			return fmt.Errorf("error in checking if repository exists: %w", err)
		} else if !exists {
			return fmt.Errorf("the repository \"%s\" does not exist", recipe.CvmfsRepo)
		}

		// Start an in-memory database
		database, err := sql.Open("sqlite3", ":memory:")
		if err != nil {
			return fmt.Errorf("unable to create in-memory database: %w", err)
		}
		defer database.Close()
		if err := db.Init(database); err != nil {
			return fmt.Errorf("unable to initialize database: %w", err)
		}

		fmt.Printf("Recipe contains %d wishes\n", len(recipe.Wishes))

		wishes := make([]db.Wish, len(recipe.Wishes))
		for i, wish := range recipe.Wishes {
			// Create wishes with output options
			wishes[i] = db.Wish{
				Identifier:    wish,
				OutputOptions: outputOptions,
			}
		}
		dbWishes, err := db.CreateWishes(nil, wishes, false)
		if err != nil {
			return fmt.Errorf("unable to create wishes: %w", err)
		}

		failedExpandWildcards := make([]db.Wish, 0)
		succededExpandWildcards := make([]db.Wish, 0)

		// Expand all wildcards
		for _, wish := range dbWishes {
			_, _, _, err := registry.ExpandWildcardAndStoreImages(wish)
			if !wish.Identifier.InputTagWildcard {
				continue
			}
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error expanding wildcard for \"\": %s\n", err)
				failedExpandWildcards = append(failedExpandWildcards, wish)
				succededExpandWildcards = append(succededExpandWildcards, wish)
			}
		}
		if len(succededExpandWildcards) > 0 {
			fmt.Printf("Successfully expanded %d wildcard wish(es)\n", len(succededExpandWildcards))
			if len(failedExpandWildcards) > 0 {
				fmt.Printf("FAILED to expand %d wildcard wish(es):\n", len(failedExpandWildcards))
				for _, wish := range failedExpandWildcards {
					fmt.Printf("\t- %s\n", wish.Identifier.InputString())
				}
			}
		}

		// Convert all images
		images, err := db.GetAllImages(nil)
		if err != nil {
			panic(fmt.Sprintf("Unable to get all images: %s", err))
		}
		fmt.Printf("Found %d images to convert\n", len(images))

		resultMutex := sync.Mutex{}
		succeded := make([]db.Image, 0)
		failed := make([]db.Image, 0)
		tasks := make([]db.TaskPtr, len(images))
		for i, image := range images {
			tasks[i], err = daemon.UpdateImageTask(nil, image)
			if err != nil {
				fmt.Printf("Error in creating the task for image %s: %s\n", image.GetSimpleName(), err)
				failed = append(failed, image)
			}
		}

		// TODO: Display some stats about the conversion. Bytes to download, number of layers, etc.

		wg := sync.WaitGroup{}
		for i, task := range tasks {
			i := i
			wg.Add(1)
			go func(task db.TaskPtr) {
				defer wg.Done()
				task.Start(nil)
				result := task.WaitUntilDone()
				if !db.TaskResultSuccessful(result) {
					fmt.Printf("Error converting image %s\n", images[i].GetSimpleName())
					resultMutex.Lock()
					failed = append(failed, images[i])
					resultMutex.Unlock()
					return
				}
				fmt.Printf("Successfully converted image %s\n", images[i].GetSimpleName())
				resultMutex.Lock()
				succeded = append(succeded, images[i])
				resultMutex.Unlock()
			}(task)
		}
		wg.Wait()

		fmt.Printf("Operation complete!\n")
		if len(succeded) > 0 {
			fmt.Printf("Successfully converted %d image(s)\n", len(succeded))
			for _, image := range succeded {
				fmt.Printf("\t- %s\n", image.GetSimpleName())
			}
		}
		if len(failed) > 0 {
			fmt.Printf("FAILED to convert %d image(s):\n", len(failed))
			for _, image := range failed {
				fmt.Printf("\t- %s\n", image.GetSimpleName())
			}
		}

		if len(failed) > 0 || len(failedExpandWildcards) > 0 {
			return fmt.Errorf("some images failed to convert")
		}

		return nil
	},
}
