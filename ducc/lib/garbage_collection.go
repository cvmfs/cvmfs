package lib

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	log "github.com/sirupsen/logrus"

	cvmfs "github.com/cvmfs/ducc/cvmfs"
	da "github.com/cvmfs/ducc/docker-api"
	l "github.com/cvmfs/ducc/log"
)

func ConstructDeleteCommands(pathsToDelete []string, pathsPerBatchCommand int, CVMFSRepo string) ([][]string, error) {

	if pathsPerBatchCommand < 1 {
		return nil, errors.New("Num of paths per batch command must be greater than zero")
	}
	repoName, _ := cvmfs.GetRepoAndSubdir(CVMFSRepo)

	// we send pathsPerBatchCommand folders to deletion at a time
	commandPrefix := []string{"cvmfs_server", "ingest"}
	commands := make([][]string, 0)
	command := commandPrefix
	for i, path := range pathsToDelete {
		path = cvmfs.PrefixRepoSubdirOnce(CVMFSRepo, path)
		if i%pathsPerBatchCommand == 0 && i > 0 {
			command = append(command, repoName)
			commands = append(commands, command)
			command = commandPrefix
		}
		command = append(command, "--fast-delete", path)
	}
	command = append(command, repoName)
	commands = append(commands, command)

	return commands, nil

}

func FindAllUsedFlatImages(CVMFSRepo string) ([]string, error) {
	root := filepath.Join("/", "cvmfs", CVMFSRepo)
	root_components := strings.Split(root, string(os.PathSeparator))
	result := make([]string, 0)

	// collectSymlinkTargets is a walker that resolves symlinks and appends
	// their real paths to result.
	collectSymlinkTargets := func(path string, info os.FileInfo, err error) error {
		if err != nil {
			l.LogE(err).WithFields(log.Fields{"path": path}).Warning("Error in opening the path, moving on.")
			return nil
		}
		if info.Mode()&os.ModeSymlink != 0 {
			realName, evalErr := filepath.EvalSymlinks(path)
			if evalErr != nil {
				return nil
			}
			result = append(result, realName)
		}
		return nil
	}

	// Walk the repo root, skipping hidden directories except .multiarch
	walker := func(path string, info os.FileInfo, err error) error {
		if err != nil {
			l.LogE(err).WithFields(log.Fields{"path": path}).Warning("Error in opening the path, moving on.")
			return nil
		}
		components := strings.Split(path, string(os.PathSeparator))
		if len(components) == len(root_components) {
			return nil
		}
		first_dir := components[len(root_components)]
		if strings.HasPrefix(first_dir, ".") {
			return filepath.SkipDir
		}
		return collectSymlinkTargets(path, info, err)
	}
	filepath.Walk(root, walker)

	// Also walk .multiarch/ to pick up architecture-specific symlinks
	// that reference flat images.
	multiarchRoot := filepath.Join(root, ".multiarch")
	filepath.Walk(multiarchRoot, collectSymlinkTargets)

	return result, nil
}

func FindAllFlatImages(CVMFSRepo string) ([]string, error) {
	root := filepath.Join("/", "cvmfs", CVMFSRepo, ".flat")
	root_components := strings.Split(root, string(os.PathSeparator))
	result := make([]string, 0)
	walker := func(path string, info os.FileInfo, err error) error {
		if err != nil {
			l.LogE(err).WithFields(log.Fields{"path": path}).Warning("Error in opening the path, moving on.")
			return nil
		}
		components := strings.Split(path, string(os.PathSeparator))
		if len(components) == len(root_components)+2 && info.IsDir() {
			result = append(result, path)
			return filepath.SkipDir
		}
		if len(components) > len(root_components)+2 {
			return filepath.SkipDir
		}
		if len(components) < len(root_components)+2 {
			return nil
		}
		// general case we keep iterating
		return nil
	}
	filepath.Walk(root, walker)
	return result, nil
}

func FindAllLayers(CVMFSRepo string) ([]string, error) {
	root := filepath.Join("/", "cvmfs", CVMFSRepo, ".layers")
	root_components := strings.Split(root, string(os.PathSeparator))
	result := make([]string, 0)
	walker := func(path string, info os.FileInfo, err error) error {
		if err != nil {
			l.LogE(err).WithFields(log.Fields{"path": path}).Warning("Error in opening the path, moving on.")
			return nil
		}
		components := strings.Split(path, string(os.PathSeparator))
		if len(components) == len(root_components)+2 && info.IsDir() {
			result = append(result, path)
			return filepath.SkipDir
		}
		if len(components) > len(root_components)+2 {
			return filepath.SkipDir
		}
		if len(components) < len(root_components)+2 {
			return nil
		}
		// general case we keep iterating
		return nil
	}
	filepath.Walk(root, walker)
	return result, nil
}

func FindAllUsedLayers(CVMFSRepo string) ([]string, error) {
	result := make([]string, 0)
	walker := func(path string, info os.FileInfo, err error) error {
		if err != nil {
			l.LogE(err).WithFields(log.Fields{"path": path}).Warning("Error in opening the path, moving on.")
			return nil
		}
		if info.Name() == "manifest.json" {
			bytes, err := ioutil.ReadFile(path)
			if err != nil {
				return filepath.SkipDir
			}
			var manifest da.Manifest
			err = json.Unmarshal(bytes, &manifest)
			if err != nil {
				return filepath.SkipDir
			}
			for _, layerStruct := range manifest.Layers {
				if layerStruct.MediaType == "application/vnd.docker.image.rootfs.foreign.diff.tar.gzip" {
					continue
				}
				layer := strings.Split(layerStruct.Digest, ":")[1]
				layerPath := filepath.Join("/", "cvmfs", CVMFSRepo, ".layers", layer[0:2], layer)
				result = append(result, layerPath)
			}
			return filepath.SkipDir
		}
		return nil
	}

	// Walk .metadata for all image manifests (including .metadata/.multiarch/)
	metadataRoot := filepath.Join("/", "cvmfs", CVMFSRepo, ".metadata")
	filepath.Walk(metadataRoot, walker)

	return result, nil
}

// FindPodmanPathsToDelete finds the podman additional image store entries that
// correspond to flat images being deleted and returns their on-disk paths for
// removal.  It also rewrites images.json and layers.json inside a single CVMFS
// transaction to drop the now-stale entries.
//
// imagesToDelete is the same slice that the caller already computed for flat
// images; each entry is an absolute path whose base name is the imageID stored
// in the podman store's images.json.
func FindPodmanPathsToDelete(CVMFSRepo string, imagesToDelete []string) ([]string, error) {
	podmanPathsToDelete := make([]string, 0)

	repoName, subDir := cvmfs.GetRepoAndSubdir(CVMFSRepo)
	storeRoot := filepath.Join("/cvmfs", repoName, subDir, podmanStoreRoot)

	imagesJSONPath := filepath.Join(storeRoot, "overlay-images", "images.json")
	layersJSONPath := filepath.Join(storeRoot, "overlay-layers", "layers.json")

	// Build the set of imageIDs being removed. Flat image paths look like
	// /cvmfs/<repo>[/<subdir>]/.flat/<xx>/<imageID>, so the base name is the ID.
	imageIDsToDelete := make(map[string]bool)
	for _, imgPath := range imagesToDelete {
		imageIDsToDelete[filepath.Base(imgPath)] = true
	}

	existingImages := readImageInfos(imagesJSONPath)
	existingLayers := readLayerInfos(layersJSONPath)

	newImages := make([]ImageInfo, 0)
	newLayers := make([]LayerInfo, 0)
	deletedLayerIDs := make(map[string]bool)

	for _, imgInfo := range existingImages {
		if !imageIDsToDelete[imgInfo.ID] {
			newImages = append(newImages, imgInfo)
			continue
		}
		// Collect all on-disk paths belonging to this store entry.
		layerID := imgInfo.Layer
		deletedLayerIDs[layerID] = true

		// Read the short-link ID so we can remove overlay/l/<linkID> as well.
		linkFilePath := filepath.Join(storeRoot, "overlay", layerID, "link")
		linkID := readLinkIDFromFile(linkFilePath)

		podmanPathsToDelete = append(podmanPathsToDelete,
			filepath.Join(storeRoot, "overlay", layerID))
		if linkID != "" {
			podmanPathsToDelete = append(podmanPathsToDelete,
				filepath.Join(storeRoot, "overlay", "l", linkID))
		}
		podmanPathsToDelete = append(podmanPathsToDelete,
			filepath.Join(storeRoot, "overlay-images", imgInfo.ID))
	}

	for _, li := range existingLayers {
		if !deletedLayerIDs[li.ID] {
			newLayers = append(newLayers, li)
		}
	}

	// Rewrite both JSON files in a single transaction.
	if len(deletedLayerIDs) > 0 {
		imagesJSON, err := json.MarshalIndent(newImages, "", " ")
		if err != nil {
			l.LogE(err).Error("Error marshaling images.json")
			return podmanPathsToDelete, err
		}
		layersJSON, err := json.MarshalIndent(newLayers, "", " ")
		if err != nil {
			l.LogE(err).Error("Error marshaling layers.json")
			return podmanPathsToDelete, err
		}
		err = cvmfs.WithinTransaction(CVMFSRepo, func() error {
			if err := os.WriteFile(imagesJSONPath, imagesJSON, 0644); err != nil {
				return fmt.Errorf("writing images.json: %w", err)
			}
			if err := os.WriteFile(layersJSONPath, layersJSON, 0644); err != nil {
				return fmt.Errorf("writing layers.json: %w", err)
			}
			return nil
		})
		if err != nil {
			l.LogE(err).Error("Error updating podman store JSON files")
			return podmanPathsToDelete, err
		}
	}

	return podmanPathsToDelete, nil
}

func FindImageToGarbageCollect(CVMFSRepo string) ([]da.Manifest, error) {
	removeSchedulePath := cvmfs.RemoveScheduleLocation(CVMFSRepo)
	llog := func(l *log.Entry) *log.Entry {
		return l.WithFields(log.Fields{
			"action": "find image to garbage collect in schedule file",
			"file":   removeSchedulePath})
	}

	var schedule []da.Manifest

	_, err := os.Stat(removeSchedulePath)
	if os.IsNotExist(err) {
		return schedule, nil
	}
	if err != nil {
		llog(l.LogE(err)).Error("Error in stating the schedule file")
		return schedule, err
	}
	scheduleFileRO, err := os.Open(removeSchedulePath)
	if err != nil {
		llog(l.LogE(err)).Error("Error in opening the schedule file")
		return schedule, err
	}

	scheduleBytes, err := ioutil.ReadAll(scheduleFileRO)
	if err != nil {
		llog(l.LogE(err)).Error("Impossible to read the schedule file")
		return schedule, err
	}

	err = scheduleFileRO.Close()
	if err != nil {
		llog(l.LogE(err)).Error("Impossible to close the schedule file")
		return schedule, err
	}

	err = json.Unmarshal(scheduleBytes, &schedule)
	if err != nil {
		llog(l.LogE(err)).Error("Impossible to unmarshal the schedule file")
		return schedule, err
	}

	return schedule, nil
}

// with image and layer we pass the digest of the layer and the digest of the image,
// both without the sha256: prefix
func GarbageCollectSingleLayer(CVMFSRepo, image, layer string) error {
	backlink, err := cvmfs.GetBacklinkFromLayer(CVMFSRepo, layer)
	llog := func(l *log.Entry) *log.Entry {
		return l.WithFields(log.Fields{"action": "garbage collect layer",
			"repo":  CVMFSRepo,
			"image": image,
			"layer": layer})
	}
	if err != nil {
		llog(l.LogE(err)).Error("Impossible to retrieve the backlink information")
		return err
	}
	var newOrigin []string
	for _, origin := range backlink.Origin {
		withoutPrefix := strings.Split(origin, ":")[1]
		if withoutPrefix != image {
			newOrigin = append(newOrigin, origin)
		}
	}
	if len(newOrigin) > 0 {
		backlink.Origin = newOrigin
		backLinkMarshall, err := json.Marshal(backlink)
		if err != nil {
			llog(l.LogE(err)).Error("Error in marshaling the new backlink")
			return err
		}

		backlinkPath := cvmfs.GetBacklinkPath(CVMFSRepo, layer)

		err = cvmfs.WithinTransaction(CVMFSRepo, func() error {
			dir := filepath.Dir(backlinkPath)
			if _, err := os.Stat(dir); os.IsNotExist(err) {
				err = os.MkdirAll(dir, 0666)
				if err != nil {
					llog(l.LogE(err)).WithFields(log.Fields{"directory": dir}).Error(
						"Error in creating the directory for the backlinks file, skipping...")
					return err
				}
			}

			err = ioutil.WriteFile(backlinkPath, backLinkMarshall, 0666)
			if err != nil {
				llog(l.LogE(err)).WithFields(log.Fields{"file": backlinkPath}).Error(
					"Error in writing the backlink file, skipping...")
				return err
			}
			return nil
		})

		if err != nil {
			llog(l.LogE(err)).Error("Error in publishing after adding the backlinks")
			return err
		}
		// write it to file
		return nil
	} else {
		err = cvmfs.RemoveLayer(CVMFSRepo, layer)
		if err != nil {
			llog(l.LogE(err)).Error("Error in deleting the layer")
		}
		return err
	}
}
