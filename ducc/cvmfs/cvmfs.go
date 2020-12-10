package cvmfs

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	copy "github.com/otiai10/copy"
	log "github.com/sirupsen/logrus"

	constants "github.com/cvmfs/ducc/constants"
	da "github.com/cvmfs/ducc/docker-api"
	temp "github.com/cvmfs/ducc/temp"
)

var dirPermision = os.FileMode(0755)
var filePermision = os.FileMode(0644)

// ingest into the repository, inside the subpath, the target (directory or file) object
// CVMFSRepo: just the name of the repository (ex: unpacked.cern.ch)
// path: the path inside the repository, without the prefix (ex: .foo/bar/baz), where to put the ingested target
// target: the path of the target in the normal FS, the thing to ingest
// if no error is returned, we remove the target from the FS
func PublishToCVMFS(CVMFSRepo string, path string, target string) (err error) {
	defer func() {
		if err == nil {
			Log().WithFields(log.Fields{"target": target, "action": "ingesting"}).Info("Deleting temporary directory")
			os.RemoveAll(target)
		}
	}()
	Log().WithFields(log.Fields{"target": target, "action": "ingesting"}).Info("Start ingesting")

	path = filepath.Join("/", "cvmfs", CVMFSRepo, path)

	Log().WithFields(log.Fields{"target": target, "action": "ingesting"}).Info("Start transaction")
	err = OpenTransaction(CVMFSRepo)
	if err != nil {
		return err
	}

	Log().WithFields(log.Fields{"target": target, "path": path, "action": "ingesting"}).Info("Copying target into path")

	targetStat, err := os.Stat(target)
	if err != nil {
		LogE(err).WithFields(log.Fields{"target": target}).Error("Impossible to obtain information about the target")
		Abort(CVMFSRepo)
		return err
	}

	if targetStat.Mode().IsDir() {
		os.RemoveAll(path)
		err = os.MkdirAll(path, dirPermision)
		if err != nil {
			LogE(err).WithFields(log.Fields{"repo": CVMFSRepo}).Warning("Error in creating the directory where to copy the singularity")
		}
		err = copy.Copy(target, path, copy.Options{PreserveTimes: true})

	} else if targetStat.Mode().IsRegular() {
		err = func() error {
			os.MkdirAll(filepath.Dir(path), dirPermision)
			os.Remove(path)

			from, err := os.Open(target)
			if err != nil {
				return err
			}
			defer from.Close()

			to, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE, filePermision)
			if err != nil {
				return err
			}
			defer to.Close()

			_, err = io.Copy(to, from)
			return err
		}()
	} else {
		err = fmt.Errorf("Trying to ingest neither a file nor a directory")
	}

	if err != nil {
		LogE(err).WithFields(log.Fields{"repo": CVMFSRepo, "target": target}).Error("Error in moving the target inside the CVMFS repo")
		Abort(CVMFSRepo)
		return err
	}

	Log().WithFields(log.Fields{"target": target, "action": "ingesting"}).Info("Publishing")
	err = Publish(CVMFSRepo)
	if err != nil {
		return err
	}
	return nil
}

// create a symbolic link inside the repository called `newLinkName`, the symlink will point to `toLinkPath`
// newLinkName: comes without the /cvmfs/$REPO/ prefix
// toLinkPath: comes without the /cvmfs/$REPO/ prefix
func CreateSymlinkIntoCVMFS(CVMFSRepo, newLinkName, toLinkPath string) (err error) {
	// add the necessary prefix
	newLinkName = filepath.Join("/", "cvmfs", CVMFSRepo, newLinkName)
	toLinkPath = filepath.Join("/", "cvmfs", CVMFSRepo, toLinkPath)

	llog := func(l *log.Entry) *log.Entry {
		return l.WithFields(log.Fields{"action": "save backlink",
			"repo":           CVMFSRepo,
			"link name":      newLinkName,
			"target to link": toLinkPath})
	}

	// check if the file we want to link actually exists
	if _, err := os.Stat(toLinkPath); os.IsNotExist(err) {
		return err
	}

	relativePath, err := filepath.Rel(newLinkName, toLinkPath)
	if err != nil {
		llog(LogE(err)).Error("Error in find the relative path")
		return err
	}
	// from the relativePath we remove the first part of the path.
	// The part we remove reprensent the same directory where is the target.
	linkChunks := strings.Split(relativePath, string(os.PathSeparator))
	link := filepath.Join(linkChunks[1:]...)

	err = OpenTransaction(CVMFSRepo)
	if err != nil {
		return err
	}

	linkDir := filepath.Dir(newLinkName)
	err = os.MkdirAll(linkDir, dirPermision)
	if err != nil {
		llog(LogE(err)).WithFields(log.Fields{
			"directory": linkDir}).Error(
			"Error in creating the directory where to store the symlink")
		Abort(CVMFSRepo)
		return err
	}

	// the symlink exists already, we delete it and replace it
	if lstat, err := os.Lstat(newLinkName); !os.IsNotExist(err) {
		if lstat.Mode()&os.ModeSymlink != 0 {
			// the file exists and it is a symlink, we overwrite it
			err = os.Remove(newLinkName)
			if err != nil {
				err = fmt.Errorf("Error in removing existsing symlink: %s", err)
				llog(LogE(err)).Error("Error in removing previous symlink")
				Abort(CVMFSRepo)
				return err
			}
		} else {
			// the file exists but is not a symlink
			err = fmt.Errorf(
				"Error, trying to overwrite with a symlink something that is not a symlink")
			llog(LogE(err)).Error("Error in creating a symlink")
			Abort(CVMFSRepo)
			return err
		}
	}

	err = os.Symlink(link, newLinkName)
	if err != nil {
		llog(LogE(err)).Error(
			"Error in creating the symlink")
		Abort(CVMFSRepo)
		return err
	}

	err = Publish(CVMFSRepo)
	if err != nil {
		return err
	}
	return nil
}

type Backlink struct {
	Origin []string `json:"origin"`
}

func GetBacklinkPath(CVMFSRepo, layerDigest string) string {
	return filepath.Join(LayerMetadataPath(CVMFSRepo, layerDigest), "origin.json")
}

func GetBacklinkFromLayer(CVMFSRepo, layerDigest string) (backlink Backlink, err error) {
	backlinkPath := GetBacklinkPath(CVMFSRepo, layerDigest)
	llog := func(l *log.Entry) *log.Entry {
		return l.WithFields(log.Fields{"action": "get backlink from layer",
			"repo":         CVMFSRepo,
			"layer":        layerDigest,
			"backlinkPath": backlinkPath})
	}

	if _, err := os.Stat(backlinkPath); os.IsNotExist(err) {
		return Backlink{Origin: []string{}}, nil
	}

	backlinkFile, err := os.Open(backlinkPath)
	if err != nil {
		llog(LogE(err)).Error(
			"Error in opening the file for writing the backlinks, skipping...")
		return
	}

	byteBackLink, err := ioutil.ReadAll(backlinkFile)
	if err != nil {
		llog(LogE(err)).Error(
			"Error in reading the bytes from the origin file, skipping...")
		return
	}

	err = backlinkFile.Close()
	if err != nil {
		llog(LogE(err)).Error(
			"Error in closing the file after reading, moving on...")
		return
	}

	err = json.Unmarshal(byteBackLink, &backlink)
	if err != nil {
		llog(LogE(err)).Error(
			"Error in unmarshaling the files, skipping...")
		return
	}
	return
}

func SaveLayersBacklink(CVMFSRepo string, imgManifest da.Manifest, imageName string, layerDigest []string) error {
	llog := func(l *log.Entry) *log.Entry {
		return l.WithFields(log.Fields{"action": "save backlink",
			"repo":  CVMFSRepo,
			"image": imageName})
	}

	llog(Log()).Info("Start saving backlinks")

	backlinks := make(map[string][]byte)

	for _, layerDigest := range layerDigest {
		imgDigest := imgManifest.Config.Digest

		backlink, err := GetBacklinkFromLayer(CVMFSRepo, layerDigest)
		if err != nil {
			llog(LogE(err)).WithFields(log.Fields{"layer": layerDigest}).Error(
				"Error in obtaining the backlink from a layer digest, skipping...")
			continue
		}
		backlink.Origin = append(backlink.Origin, imgDigest)

		backlinkBytesMarshal, err := json.Marshal(backlink)
		if err != nil {
			llog(LogE(err)).WithFields(log.Fields{"layer": layerDigest}).Error(
				"Error in Marshaling back the files, skipping...")
			continue
		}

		backlinkPath := GetBacklinkPath(CVMFSRepo, layerDigest)
		backlinks[backlinkPath] = backlinkBytesMarshal
	}

	llog(Log()).Info("Start transaction")
	err := OpenTransaction(CVMFSRepo)
	if err != nil {
		return err
	}

	for path, fileContent := range backlinks {
		// the path may not be there, check, and if it doesn't exists create it
		dir := filepath.Dir(path)
		if _, err := os.Stat(dir); os.IsNotExist(err) {
			err = os.MkdirAll(dir, dirPermision)
			if err != nil {
				llog(LogE(err)).WithFields(log.Fields{"file": path}).Error(
					"Error in creating the directory for the backlinks file, skipping...")
				continue
			}
		}
		err = ioutil.WriteFile(path, fileContent, filePermision)
		if err != nil {
			llog(LogE(err)).WithFields(log.Fields{"file": path}).Error(
				"Error in writing the backlink file, skipping...")
			continue
		}
		llog(LogE(err)).WithFields(log.Fields{"file": path}).Info(
			"Wrote backlink")
	}

	err = Publish(CVMFSRepo)
	if err != nil {
		return err
	}

	return nil
}

func RemoveScheduleLocation(CVMFSRepo string) string {
	return filepath.Join("/", "cvmfs", CVMFSRepo, ".metadata", "remove-schedule.json")
}

func AddManifestToRemoveScheduler(CVMFSRepo string, manifest da.Manifest) error {
	schedulePath := RemoveScheduleLocation(CVMFSRepo)
	llog := func(l *log.Entry) *log.Entry {
		return l.WithFields(log.Fields{
			"action": "add manifest to remove schedule",
			"file":   schedulePath})
	}
	var schedule []da.Manifest

	// if the file exist, load from it
	if _, err := os.Stat(schedulePath); !os.IsNotExist(err) {

		scheduleFileRO, err := os.OpenFile(schedulePath, os.O_RDONLY, filePermision)
		if err != nil {
			llog(LogE(err)).Error("Impossible to open the schedule file")
			return err
		}

		scheduleBytes, err := ioutil.ReadAll(scheduleFileRO)
		if err != nil {
			llog(LogE(err)).Error("Impossible to read the schedule file")
			return err
		}

		err = scheduleFileRO.Close()
		if err != nil {
			llog(LogE(err)).Error("Impossible to close the schedule file")
			return err
		}

		err = json.Unmarshal(scheduleBytes, &schedule)
		if err != nil {
			llog(LogE(err)).Error("Impossible to unmarshal the schedule file")
			return err
		}
	}

	schedule = func() []da.Manifest {
		for _, m := range schedule {
			if m.Config.Digest == manifest.Config.Digest {
				return schedule
			}
		}
		schedule = append(schedule, manifest)
		return schedule
	}()

	err := OpenTransaction(CVMFSRepo)
	if err != nil {
		return err
	}

	if _, err = os.Stat(schedulePath); os.IsNotExist(err) {
		err = os.MkdirAll(filepath.Dir(schedulePath), dirPermision)
		if err != nil {
			llog(LogE(err)).Error("Error in creating the directory where save the schedule")
		}
	}

	bytes, err := json.Marshal(schedule)
	if err != nil {
		llog(LogE(err)).Error("Error in marshaling the new schedule")
	} else {

		err = ioutil.WriteFile(schedulePath, bytes, filePermision)
		if err != nil {
			llog(LogE(err)).Error("Error in writing the new schedule")
		} else {
			llog(Log()).Info("Wrote new remove schedule")
		}
	}

	err = Publish(CVMFSRepo)
	if err != nil {
		return err
	}

	return nil
}

func RemoveSingularityImageFromManifest(CVMFSRepo string, manifest da.Manifest) error {
	dir := filepath.Join("/", "cvmfs", CVMFSRepo, manifest.GetSingularityPath())
	llog := func(l *log.Entry) *log.Entry {
		return l.WithFields(log.Fields{
			"action": "removing singularity directory", "directory": dir})
	}
	err := RemoveDirectory(CVMFSRepo, manifest.GetSingularityPath())
	if err != nil {
		llog(LogE(err)).Error("Error in removing singularity direcotry")
		return err
	}
	return nil
}

func LayerPath(CVMFSRepo, layerDigest string) string {
	return filepath.Join("/", "cvmfs", CVMFSRepo, constants.SubDirInsideRepo, layerDigest[0:2], layerDigest)
}

func LayerRootfsPath(CVMFSRepo, layerDigest string) string {
	return filepath.Join(LayerPath(CVMFSRepo, layerDigest), "layerfs")
}

func LayerMetadataPath(CVMFSRepo, layerDigest string) string {
	return filepath.Join(LayerPath(CVMFSRepo, layerDigest), ".metadata")
}

//from /cvmfs/$REPO/foo/bar -> foo/bar
func TrimCVMFSRepoPrefix(path string) string {
	return strings.Join(strings.Split(path, string(os.PathSeparator))[3:], string(os.PathSeparator))
}

func RemoveLayer(CVMFSRepo, layerDigest string) error {
	llog := func(l *log.Entry) *log.Entry {
		return l.WithFields(log.Fields{
			"action": "removing layer", "layer": layerDigest})
	}
	err := RemoveDirectory(CVMFSRepo, constants.SubDirInsideRepo, layerDigest[0:2], layerDigest)
	if err != nil {
		llog(LogE(err)).Error("Error in deleting a layer")
		return err
	}
	return nil
}

func RemoveDirectory(CVMFSRepo string, dirPath ...string) error {
	path := []string{"/cvmfs", CVMFSRepo}
	for _, p := range dirPath {
		path = append(path, p)
	}
	directory := filepath.Join(path...)
	llog := func(l *log.Entry) *log.Entry {
		return l.WithFields(log.Fields{
			"action": "removing directory", "directory": directory})
	}
	stat, err := os.Stat(directory)
	if err != nil {
		if os.IsNotExist(err) {
			llog(LogE(err)).Warning("Directory not existing")
			return nil
		}
		llog(LogE(err)).Error("Error in stating the directory")
		return err
	}
	if !stat.Mode().IsDir() {
		err = fmt.Errorf("Trying to remove something different from a directory")
		llog(LogE(err)).Error("Error, input is not a directory")
		return err
	}

	dirsSplitted := strings.Split(directory, string(os.PathSeparator))
	if len(dirsSplitted) <= 3 || dirsSplitted[1] != "cvmfs" {
		err := fmt.Errorf("Directory not in the CVMFS repo")
		llog(LogE(err)).Error("Error in opening the transaction")
		return err
	}
	err = OpenTransaction(CVMFSRepo)
	if err != nil {
		return err
	}

	err = os.RemoveAll(directory)
	if err != nil {
		llog(LogE(err)).Error("Error in publishing after adding the backlinks")
		Abort(CVMFSRepo)
		return err
	}

	err = Publish(CVMFSRepo)
	if err != nil {
		return err
	}

	return nil
}

func CreateCatalogIntoDir(CVMFSRepo, dir string) (err error) {
	catalogPath := filepath.Join("/", "cvmfs", CVMFSRepo, dir, ".cvmfscatalog")
	if _, err := os.Stat(catalogPath); os.IsNotExist(err) {
		tmpFile, err := ioutil.TempFile("", "tempCatalog")
		tmpFile.Close()
		if err != nil {
			return err
		}
		err = PublishToCVMFS(CVMFSRepo, TrimCVMFSRepoPrefix(catalogPath), tmpFile.Name())
		if err != nil {
			return err
		}
		return err
	}
	return nil
}

//writes data to file and publish in cvmfs repo path
func WriteDataToCvmfs(CVMFSRepo, path string, data []byte) (err error) {
	tmpFile, err := temp.UserDefinedTempFile()
	if err != nil {
		LogE(err).Error("Error in creating temporary file for writing data")
		return err
	}
	defer os.RemoveAll(tmpFile.Name())
	err = ioutil.WriteFile(tmpFile.Name(), data, 0644)
	if err != nil {
		LogE(err).Error("Error in writing data to file")
		return err
	}
	err = tmpFile.Close()
	if err != nil {
		LogE(err).Error("Error in closing temporary file")
		return err
	}
	return PublishToCVMFS(CVMFSRepo, path, tmpFile.Name())
}

func LogE(err error) *log.Entry {
	return log.WithFields(log.Fields{"error": err})
}

func Log() *log.Entry {
	return log.WithFields(log.Fields{})
}
