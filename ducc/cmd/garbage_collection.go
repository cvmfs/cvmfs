package cmd

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	exec "github.com/cvmfs/ducc/exec"
	"github.com/cvmfs/ducc/lib"
	l "github.com/cvmfs/ducc/log"
)

const (
	deleteBatch = 50
)

var (
	dryRun bool
)

func init() {
	garbageCollectionCmd.Flags().BoolVarP(&dryRun, "dry-run", "n", false, "Dry run the garbage collection")
	rootCmd.AddCommand(garbageCollectionCmd)
}

var garbageCollectionCmd = &cobra.Command{
	Use:     "garbage-collection",
	Short:   "Removes layers that are not necessary anymore",
	Aliases: []string{"gc"},
	Args:    cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		CVMFSRepo := args[0]

		llog := func(l *log.Entry) *log.Entry {
			return l.WithFields(log.Fields{"action": "garbage collect",
				"repo": CVMFSRepo,
			})
		}

		// tried already to make them in parallel, we don't gain much
		// from ~1min to ~30 sec
		llog(l.Log()).Info("Scanning images to delete")
		imagesUsed, _ := lib.FindAllUsedFlatImages(CVMFSRepo)
		imagesAll, _ := lib.FindAllFlatImages(CVMFSRepo)

		llog(l.Log()).Info("Scanning layers to delete")
		layersUsed, _ := lib.FindAllUsedLayers(CVMFSRepo)
		layersAll, _ := lib.FindAllLayers(CVMFSRepo)

		llog(l.Log()).Info("Scanning completed. Computing paths to delete.")

		// we first figure out all the unique paths that are used
		imagesUsedMap := make(map[string]bool)
		for _, image := range imagesUsed {
			imagesUsedMap[image] = true
		}
		layersUsedMap := make(map[string]bool)
		for _, layer := range layersUsed {
			layersUsedMap[layer] = true
		}

		// we figure out what path is not necessary anymore
		imagesToDelete := make([]string, 0)
		for _, candidateDelete := range imagesAll {
			if imagesUsedMap[candidateDelete] {
				continue
			}
			imagesToDelete = append(imagesToDelete, candidateDelete)
		}
		layersToDelete := make([]string, 0)
		for _, candidateDelete := range layersAll {
			if layersUsedMap[candidateDelete] {
				continue
			}
			layersToDelete = append(layersToDelete, candidateDelete)
		}
		podmanPathsToDelete, _ := lib.FindPodmanPathsToDelete(CVMFSRepo, layersToDelete)

		// we remove the prefix to the paths and we accumulate them in a single array
		// we remove the prefix to pass them to `cvmfs_server ingest --delete $path_with_no_prefix CVMFSRepo`
		prefix := filepath.Join("/", "cvmfs", CVMFSRepo) + "/"
		today := time.Now()

		pathShouldBeDeleted := func(path string) bool {
			if !strings.HasPrefix(path, prefix) {
				llog(l.Log()).WithFields(log.Fields{"path": path, "prefix": prefix}).Warning("Path does not have the expected prefix")
				return false
			}
			stat, err := os.Stat(path)
			if err != nil {
				llog(l.Log()).WithFields(log.Fields{"path": path, "err": err}).Warning("Error in stating the path")
				return false
			}
			modTime := stat.ModTime()
			thirtyDays := 30 * 24 * time.Hour
			if modTime.Add(thirtyDays).After(today) {
				llog(l.Log()).WithFields(log.Fields{"path": path, "grace period": "30 days", "path mod time": modTime}).Warning("Path still in its grace period")
				return false
			}
			return true
		}

		pathsToDelete := make([]string, 0)
		for _, path := range imagesToDelete {
			if pathShouldBeDeleted(path) {
				pathsToDelete = append(pathsToDelete, strings.TrimPrefix(path, prefix))
			}
		}
		for _, path := range layersToDelete {
			if pathShouldBeDeleted(path) {
				pathsToDelete = append(pathsToDelete, strings.TrimPrefix(path, prefix))
			}
		}
		for _, path := range podmanPathsToDelete {
			if pathShouldBeDeleted(path) {
				pathsToDelete = append(pathsToDelete, strings.TrimPrefix(path, prefix))
			}
		}

		llog(l.Log()).WithFields(log.Fields{"num. of path to delete": len(pathsToDelete)}).Info("Ready to delete paths")

		// we send 50 folder to deletion at the time
		commandPrefix := []string{"cvmfs_server", "ingest"}
		commands := make([][]string, 0)
		command := commandPrefix
		j := 0
		for i, path := range pathsToDelete {
			j = i
			if i%deleteBatch == 0 && i > 0 {
				command = append(command, CVMFSRepo)
				commands = append(commands, command)
				command = commandPrefix
				continue
			}
			command = append(command, "--delete", path)
		}
		if j%deleteBatch != 0 && j > 0 {
			command = append(command, CVMFSRepo)
			commands = append(commands, command)
		}

		if dryRun {
			fmt.Printf("Dry run for garbage collection\n")
			fmt.Printf("It would execute the following commands:\n\n")
		}
		for _, cmd := range commands {
			if dryRun {
				fmt.Printf("%v\n", cmd)
			} else {
				exec.ExecCommand(cmd...).Start()
			}
		}
	},
}
