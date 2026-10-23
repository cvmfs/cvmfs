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
	gracePeriod int
)

var (
	dryRun     bool
	scanOnly   bool
	deleteOnly bool
	dbPath     string
)

func init() {
	garbageCollectionCmd.Flags().BoolVar(&dryRun, "dry-run", false, "Dry run the garbage collection")
	garbageCollectionCmd.Flags().IntVar(&gracePeriod, "grace-period", 0, "Grace period for which to keep unused path (days)")
	garbageCollectionCmd.Flags().BoolVar(&scanOnly, "scan-only", false, "Only scan for paths to delete and store them in the database; do not perform deletions")
	garbageCollectionCmd.Flags().BoolVar(&deleteOnly, "delete-only", false, "Only delete paths previously recorded in the database; do not scan")
	garbageCollectionCmd.Flags().StringVar(&dbPath, "db-path", "", "Path to the SQLite database for recording paths to delete (default: gc_<repo>.db in current directory)")
	rootCmd.AddCommand(garbageCollectionCmd)
}

var garbageCollectionCmd = &cobra.Command{
	Use:     "garbage-collection <cvmfs repo>",
	Short:   "Removes layers that are not necessary anymore",
	Aliases: []string{"gc"},
	Args:    cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		CVMFSRepo := args[0]

		if scanOnly && deleteOnly {
			fmt.Fprintf(os.Stderr, "Error: --scan-only and --delete-only are mutually exclusive\n")
			os.Exit(1)
		}

		llog := func(l *log.Entry) *log.Entry {
			return l.WithFields(log.Fields{"action": "garbage collect",
				"repo": CVMFSRepo,
			})
		}

		// Determine the database path
		effectiveDBPath := dbPath
		if effectiveDBPath == "" {
			// Sanitize repo name for use in filename
			safeRepo := strings.ReplaceAll(CVMFSRepo, "/", "_")
			effectiveDBPath = fmt.Sprintf("gc_%s.db", safeRepo)
		}

		llog(l.Log()).WithFields(log.Fields{"db": effectiveDBPath}).Info("Using GC database")

		gcDB, err := lib.OpenGCDatabase(effectiveDBPath)
		if err != nil {
			llog(l.Log()).WithFields(log.Fields{"err": err}).Error("Failed to open GC database")
			os.Exit(1)
		}
		defer gcDB.Close()

		// --- Scan phase ---
		if !deleteOnly {
			// Read and store repository metadata before scanning
			meta, err := lib.GetRepoMetadata(CVMFSRepo)
			if err != nil {
				llog(l.Log()).WithFields(log.Fields{"err": err}).Warning("Failed to read repository metadata from xattrs")
			} else {
				llog(l.Log()).WithFields(log.Fields{
					"repo_name":         meta.Name,
					"revision":          meta.Revision,
					"root_catalog_hash": meta.RootCatalogHash,
				}).Info("Repository metadata at scan time")
				if err := gcDB.SaveRepoMetadata(meta); err != nil {
					llog(l.Log()).WithFields(log.Fields{"err": err}).Error("Failed to save repository metadata to GC database")
				}
			}
			scanPaths(CVMFSRepo, gcDB, llog)
		}

		// --- Summary ---
		pending, deleted, err := gcDB.Summary()
		if err != nil {
			llog(l.Log()).WithFields(log.Fields{"err": err}).Error("Failed to get database summary")
			os.Exit(1)
		}
		llog(l.Log()).WithFields(log.Fields{
			"pending": pending,
			"deleted": deleted,
		}).Info("GC database summary")

		// Log stored repo metadata (useful in --delete-only mode)
		if deleteOnly {
			meta, err := gcDB.GetRepoMetadataFromDB()
			if err == nil {
				llog(l.Log()).WithFields(log.Fields{
					"repo_name":         meta.Name,
					"revision":          meta.Revision,
					"root_catalog_hash": meta.RootCatalogHash,
				}).Info("Repository metadata recorded at scan time")
			}
		}

		if scanOnly {
			llog(l.Log()).Info("Scan-only mode: skipping deletions")
			return
		}

		// --- Delete phase ---
		deletePaths(CVMFSRepo, gcDB, effectiveDBPath, llog)
	},
}

// scanPaths discovers unused images, layers and podman paths, applies the
// grace period filter, and records the paths-to-delete in the GC database.
func scanPaths(CVMFSRepo string, gcDB *lib.GCDatabase, llog func(*log.Entry) *log.Entry) {
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
	podmanPathsToDelete, _ := lib.FindPodmanPathsToDelete(CVMFSRepo, imagesToDelete)

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
		gracePeriodInHours := time.Duration(gracePeriod) * 24 * time.Hour
		if modTime.Add(gracePeriodInHours).After(today) {
			llog(l.Log()).WithFields(log.Fields{"path": path, "grace period [days]": gracePeriod, "path mod time": modTime}).Warning("Path still in its grace period")
			return false
		}
		return true
	}

	// Collect filtered paths by category
	imagePathsFiltered := make([]string, 0)
	for _, path := range imagesToDelete {
		if pathShouldBeDeleted(path) {
			imagePathsFiltered = append(imagePathsFiltered, strings.TrimPrefix(path, prefix))
		}
	}
	layerPathsFiltered := make([]string, 0)
	for _, path := range layersToDelete {
		if pathShouldBeDeleted(path) {
			layerPathsFiltered = append(layerPathsFiltered, strings.TrimPrefix(path, prefix))
		}
	}
	podmanPathsFiltered := make([]string, 0)
	for _, path := range podmanPathsToDelete {
		if pathShouldBeDeleted(path) {
			podmanPathsFiltered = append(podmanPathsFiltered, strings.TrimPrefix(path, prefix))
		}
	}

	// Store in database
	if err := gcDB.InsertPaths(imagePathsFiltered, "image"); err != nil {
		llog(l.Log()).WithFields(log.Fields{"err": err}).Error("Failed to insert image paths into GC database")
	}
	if err := gcDB.InsertPaths(layerPathsFiltered, "layer"); err != nil {
		llog(l.Log()).WithFields(log.Fields{"err": err}).Error("Failed to insert layer paths into GC database")
	}
	if err := gcDB.InsertPaths(podmanPathsFiltered, "podman"); err != nil {
		llog(l.Log()).WithFields(log.Fields{"err": err}).Error("Failed to insert podman paths into GC database")
	}

	llog(l.Log()).WithFields(log.Fields{
		"images": len(imagePathsFiltered),
		"layers": len(layerPathsFiltered),
		"podman": len(podmanPathsFiltered),
	}).Info("Paths recorded in GC database")
}

// deletePaths uses `cvmfs_server ingest --gc-db` to delete all pending paths
// from the GC database in a single CVMFS transaction.  The swissknife ingest
// command reads the paths directly from the database and marks them as deleted
// after a successful commit.
func deletePaths(CVMFSRepo string, gcDB *lib.GCDatabase, gcDBPath string, llog func(*log.Entry) *log.Entry) {
	pathsToDelete, err := gcDB.PendingPaths()
	if err != nil {
		llog(l.Log()).WithFields(log.Fields{"err": err}).Error("Failed to read pending paths from GC database")
		os.Exit(1)
	}

	if len(pathsToDelete) == 0 {
		llog(l.Log()).Info("No pending paths to delete")
		return
	}

	llog(l.Log()).WithFields(log.Fields{"num. of path to delete": len(pathsToDelete)}).Info("Ready to delete paths")

	cmd := lib.ConstructGCDBDeleteCommand(gcDBPath, CVMFSRepo)

	if dryRun {
		fmt.Printf("Dry run for garbage collection\n")
		fmt.Printf("It would execute the following command:\n\n")
		fmt.Printf("%v\n", cmd)
		return
	}

	err = exec.ExecCommand(cmd...).Start()
	if err != nil {
		llog(l.Log()).WithFields(log.Fields{"err": err, "cmd": cmd}).Error("Error executing delete command")
		os.Exit(1)
	}

	// Note: cvmfs_server ingest --gc-db marks paths as deleted in the DB
	// after successful publication. Verify the result.
	pending, _, err := gcDB.Summary()
	if err != nil {
		llog(l.Log()).WithFields(log.Fields{"err": err}).Warning("Failed to verify GC database after deletion")
	} else if pending > 0 {
		llog(l.Log()).WithFields(log.Fields{"remaining_pending": pending}).Warning("Some paths may not have been marked as deleted")
	} else {
		llog(l.Log()).Info("All paths successfully deleted and marked in GC database")
	}
}
