package lib

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	log "github.com/sirupsen/logrus"

	da "github.com/cvmfs/ducc/docker-api"
)

func FindAllUsedFlatImages(CVMFSRepo string) ([]string, error) {
	root := filepath.Join("/", "cvmfs", CVMFSRepo)
	root_components := strings.Split(root, string(os.PathSeparator))
	result := make([]string, 0)
	walker := func(path string, info os.FileInfo, err error) error {
		// some kind of error, we don't really care and we just move on.
		if err != nil {
			LogE(err).WithFields(log.Fields{"path": path}).Warning("Error in opening the path, moving on.")
			return nil
		}
		components := strings.Split(path, string(os.PathSeparator))
		// first root directory, not sure if this ever happen
		if len(components) == len(root_components) {
			return nil
		}
		// checking if we are in a hidden directory
		// if we are, we skip it all
		first_dir := components[len(root_components)]
		if strings.HasPrefix(first_dir, ".") {
			return filepath.SkipDir
		}
		// let's check if we have reach a symlink
		// if we are in a symlink, we should capture
		// the image digest
		// we don't need to break the walk since Walk
		// does not follow symlinks
		if info.Mode()&os.ModeSymlink != 0 {
			realName, _ := filepath.EvalSymlinks(path)
			if err != nil {
				return nil
			}
			result = append(result, realName)
		}
		// general case we keep iterating
		return nil
	}
	filepath.Walk(root, walker)
	return result, nil
}

func FindAllFlatImages(CVMFSRepo string) ([]string, error) {
	root := filepath.Join("/", "cvmfs", CVMFSRepo, ".flat")
	root_components := strings.Split(root, string(os.PathSeparator))
	result := make([]string, 0)
	walker := func(path string, info os.FileInfo, err error) error {
		if err != nil {
			LogE(err).WithFields(log.Fields{"path": path}).Warning("Error in opening the path, moving on.")
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
			LogE(err).WithFields(log.Fields{"path": path}).Warning("Error in opening the path, moving on.")
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
	root := filepath.Join("/", "cvmfs", CVMFSRepo, ".metadata")
	result := make([]string, 0)
	walker := func(path string, info os.FileInfo, err error) error {
		if err != nil {
			LogE(err).WithFields(log.Fields{"path": path}).Warning("Error in opening the path, moving on.")
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
				layer := strings.Split(layerStruct.Digest, ":")[1]
				layerPath := filepath.Join("/", "cvmfs", CVMFSRepo, ".layers", layer[0:2], layer)
				result = append(result, layerPath)
			}
			return filepath.SkipDir
		}
		return nil
	}
	filepath.Walk(root, walker)
	return result, nil
}

func FindImageToGarbageCollect(CVMFSRepo string) ([]da.Manifest, error) {
	removeSchedulePath := RemoveScheduleLocation(CVMFSRepo)
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
		llog(LogE(err)).Error("Error in stating the schedule file")
		return schedule, err
	}
	scheduleFileRO, err := os.Open(removeSchedulePath)
	if err != nil {
		llog(LogE(err)).Error("Error in opening the schedule file")
		return schedule, err
	}

	scheduleBytes, err := ioutil.ReadAll(scheduleFileRO)
	if err != nil {
		llog(LogE(err)).Error("Impossible to read the schedule file")
		return schedule, err
	}

	err = scheduleFileRO.Close()
	if err != nil {
		llog(LogE(err)).Error("Impossible to close the schedule file")
		return schedule, err
	}

	err = json.Unmarshal(scheduleBytes, &schedule)
	if err != nil {
		llog(LogE(err)).Error("Impossible to unmarshal the schedule file")
		return schedule, err
	}

	return schedule, nil
}

// with image and layer we pass the digest of the layer and the digest of the image,
// both without the sha256: prefix
func GarbageCollectSingleLayer(CVMFSRepo, image, layer string) error {
	backlink, err := getBacklinkFromLayer(CVMFSRepo, layer)
	llog := func(l *log.Entry) *log.Entry {
		return l.WithFields(log.Fields{"action": "garbage collect layer",
			"repo":  CVMFSRepo,
			"image": image,
			"layer": layer})
	}
	if err != nil {
		llog(LogE(err)).Error("Impossible to retrieve the backlink information")
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
			llog(LogE(err)).Error("Error in marshaling the new backlink")
			return err
		}

		backlinkPath := getBacklinkPath(CVMFSRepo, layer)

		err = ExecCommand("cvmfs_server", "transaction", CVMFSRepo).Start()
		if err != nil {
			llog(LogE(err)).Error("Error in opening the transaction")
			return err
		}

		dir := filepath.Dir(backlinkPath)
		if _, err := os.Stat(dir); os.IsNotExist(err) {
			err = os.MkdirAll(dir, 0666)
			if err != nil {
				llog(LogE(err)).WithFields(log.Fields{"directory": dir}).Error(
					"Error in creating the directory for the backlinks file, skipping...")
				return err
			}
		}

		err = ioutil.WriteFile(backlinkPath, backLinkMarshall, 0666)
		if err != nil {
			llog(LogE(err)).WithFields(log.Fields{"file": backlinkPath}).Error(
				"Error in writing the backlink file, skipping...")
			return err
		}

		err = ExecCommand("cvmfs_server", "publish", CVMFSRepo).Start()
		if err != nil {
			llog(LogE(err)).Error("Error in publishing after adding the backlinks")
			return err
		}
		// write it to file
		return nil
	} else {
		err = RemoveLayer(CVMFSRepo, layer)
		if err != nil {
			llog(LogE(err)).Error("Error in deleting the layer")
		}
		return err
	}
}
