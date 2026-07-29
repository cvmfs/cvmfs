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
	exec "github.com/cvmfs/ducc/exec"
	l "github.com/cvmfs/ducc/log"
	temp "github.com/cvmfs/ducc/temp"
)

// ingest into the repository, inside the subpath, the target (directory or file) object
// CVMFSRepo: just the name of the repository (ex: unpacked.cern.ch)
// path: the path inside the repository, without the prefix (ex: .foo/bar/baz), where to put the ingested target
// target: the path of the target in the normal FS, the thing to ingest
// if no error is returned, we remove the target from the FS
func PublishToCVMFS(CVMFSRepo string, path string, target string) (err error) {
	return PublishToCVMFSWithLogger(nil, CVMFSRepo, path, target)
}

func PublishToCVMFSWithLogger(logger *log.Entry, CVMFSRepo string, path string, target string) (err error) {
	logger = l.Ensure(logger)
	opLogger := logger.WithFields(log.Fields{"target": target, "action": "ingesting"})
	defer func() {
		if err == nil {
			opLogger.Trace("Deleting temporary directory")
			os.RemoveAll(target)
		}
	}()
	opLogger.Trace("Start ingesting")

	if MountlessPublishing() {
		// The deferred cleanup above removes target on success (err == nil).
		err = publishToCVMFSMountless(logger, CVMFSRepo, path, target)
		return err
	}

	repoName, _ := GetRepoAndSubdir(CVMFSRepo)
	path = PrefixRepoSubdirOnce(CVMFSRepo, path)
	path = filepath.Join("/", "cvmfs", repoName, path)

	opLogger.Trace("Start transaction")

	err = WithinTransactionWithLogger(logger, CVMFSRepo, func() error {
		opLogger.WithField("path", path).Trace("Copying target into path")

		targetStat, err := os.Stat(target)
		if err != nil {
			opLogger.WithField("error", err).Error("Impossible to obtain information about the target")
			return err
		}

		if targetStat.Mode().IsDir() {
			os.RemoveAll(path)
			err = os.MkdirAll(path, constants.DirPermision)
			if err != nil {
				opLogger.WithFields(log.Fields{"repo": CVMFSRepo, "error": err}).Warning("Error in creating the directory where to copy the singularity")
			}
			err = copy.Copy(target, path, copy.Options{PreserveTimes: true})

		} else if targetStat.Mode().IsRegular() {
			err = func() error {
				os.MkdirAll(filepath.Dir(path), constants.DirPermision)
				os.Remove(path)

				from, err := os.Open(target)
				if err != nil {
					return err
				}
				defer from.Close()

				to, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE, constants.FilePermision)
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
			opLogger.WithFields(log.Fields{"repo": CVMFSRepo, "error": err}).Error("Error in moving the target inside the CVMFS repo")
			return err
		}
		return nil
	})

	return err
}

// create a symbolic link inside the repository called `newLinkName`, the symlink will point to `toLinkPath`
// newLinkName: comes without the /cvmfs/$REPO/ prefix
// toLinkPath: comes without the /cvmfs/$REPO/ prefix
func CreateSymlinkIntoCVMFS(CVMFSRepo, newLinkName, toLinkPath string) (err error) {
	return CreateSymlinkIntoCVMFSWithLogger(nil, CVMFSRepo, newLinkName, toLinkPath)
}

func CreateSymlinkIntoCVMFSWithLogger(logger *log.Entry, CVMFSRepo, newLinkName, toLinkPath string) (err error) {
	logger = l.Ensure(logger)
	// repository-relative link path (kept for the mountless ingest path)
	repoRelLink := newLinkName
	// add the necessary prefix
	newLinkName = filepath.Join("/", "cvmfs", CVMFSRepo, newLinkName)
	toLinkPath = filepath.Join("/", "cvmfs", CVMFSRepo, toLinkPath)

	llog := func(entry *log.Entry) *log.Entry {
		return l.Ensure(entry).WithFields(log.Fields{"action": "create symlink",
			"repo":           CVMFSRepo,
			"link name":      newLinkName,
			"target to link": toLinkPath})
	}

	// Compute the relative link target (pure path arithmetic, no I/O).
	relativePath, err := filepath.Rel(newLinkName, toLinkPath)
	if err != nil {
		llog(l.LogE(err)).Error("Error in find the relative path")
		return err
	}
	// from the relativePath we remove the first part of the path.
	// The part we remove represents the same directory where is the target.
	linkChunks := strings.Split(relativePath, string(os.PathSeparator))
	link := filepath.Join(linkChunks[1:]...)

	if MountlessPublishing() {
		// No mount to stat the target against; create the link via ingest.
		return createSymlinkMountless(logger, CVMFSRepo, repoRelLink, link)
	}

	// check if the file we want to link actually exists
	if _, err := os.Stat(toLinkPath); os.IsNotExist(err) {
		return err
	}

	err = WithinTransactionWithLogger(logger, CVMFSRepo, func() error {
		linkDir := filepath.Dir(newLinkName)
		err = os.MkdirAll(linkDir, constants.DirPermision)
		if err != nil {
			llog(l.LogE(err)).WithFields(log.Fields{
				"directory": linkDir}).Error(
				"Error in creating the directory where to store the symlink")
			return err
		}

		// the symlink exists already, we delete it and replace it
		if lstat, err := os.Lstat(newLinkName); !os.IsNotExist(err) {
			if lstat.Mode()&os.ModeSymlink != 0 {
				// the file exists and it is a symlink, we overwrite it
				err = os.Remove(newLinkName)
				if err != nil {
					err = fmt.Errorf("Error in removing existing symlink: %s", err)
					llog(l.LogE(err)).Error("Error in removing previous symlink")
					return err
				}
			} else {
				// the file exists but is not a symlink
				err = fmt.Errorf(
					"Error, trying to overwrite with a symlink something that is not a symlink")
				llog(l.LogE(err)).Error("Error in creating a symlink")
				return err
			}
		}

		err = os.Symlink(link, newLinkName)
		if err != nil {
			llog(l.LogE(err)).Error(
				"Error in creating the symlink")
			return err
		}
		return nil
	})

	return err
}

// CreateVariantSymlinkIntoCVMFS creates a CVMFS variant symlink whose target
// is stored verbatim (without path resolution or existence checks).  The
// rawTarget may contain CVMFS variable-expansion expressions such as
// $(CVMFS_ARCH:-amd64); the CVMFS client will expand them at read time.
//
// newLinkName is the symlink path relative to the repository root
// (without the /cvmfs/<repo>/ prefix).  rawTarget is the exact string that
// will be stored as the symlink destination.
func CreateVariantSymlinkIntoCVMFS(CVMFSRepo, newLinkName, rawTarget string) error {
	return CreateVariantSymlinkIntoCVMFSWithLogger(nil, CVMFSRepo, newLinkName, rawTarget)
}

func CreateVariantSymlinkIntoCVMFSWithLogger(logger *log.Entry, CVMFSRepo, newLinkName, rawTarget string) (err error) {
	logger = l.Ensure(logger)
	fullLinkName := filepath.Join("/", "cvmfs", CVMFSRepo, newLinkName)

	llog := func(entry *log.Entry) *log.Entry {
		return l.Ensure(entry).WithFields(log.Fields{
			"action":    "create variant symlink",
			"repo":      CVMFSRepo,
			"link name": fullLinkName,
			"target":    rawTarget,
		})
	}

	if MountlessPublishing() {
		// rawTarget is stored verbatim (it may contain $(...) expansions).
		return createSymlinkMountless(logger, CVMFSRepo, newLinkName, rawTarget)
	}

	err = WithinTransactionWithLogger(logger, CVMFSRepo, func() error {
		linkDir := filepath.Dir(fullLinkName)
		if mkErr := os.MkdirAll(linkDir, constants.DirPermision); mkErr != nil {
			llog(l.LogE(mkErr)).WithField("directory", linkDir).
				Error("Error in creating the directory for the variant symlink")
			return mkErr
		}

		// If the symlink already exists with the identical target, skip.
		if existing, readErr := os.Readlink(fullLinkName); readErr == nil && existing == rawTarget {
			llog(logger).Trace("Variant symlink already up to date, skipping")
			return nil
		}

		// Remove any existing symlink; refuse to overwrite a real file/dir.
		if lstat, lstatErr := os.Lstat(fullLinkName); !os.IsNotExist(lstatErr) {
			if lstat.Mode()&os.ModeSymlink != 0 {
				if rmErr := os.Remove(fullLinkName); rmErr != nil {
					err := fmt.Errorf("error removing existing variant symlink: %s", rmErr)
					llog(l.LogE(err)).Error("Error removing existing variant symlink")
					return err
				}
			} else {
				err := fmt.Errorf("refusing to overwrite non-symlink with variant symlink at %s", fullLinkName)
				llog(l.LogE(err)).Error("Error creating variant symlink")
				return err
			}
		}

		if symErr := os.Symlink(rawTarget, fullLinkName); symErr != nil {
			llog(l.LogE(symErr)).Error("Error in creating the variant symlink")
			return symErr
		}
		llog(logger).Info("Created variant symlink")
		return nil
	})

	return err
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
		llog(l.LogE(err)).Error(
			"Error in opening the file for writing the backlinks, skipping...")
		return
	}

	byteBackLink, err := ioutil.ReadAll(backlinkFile)
	if err != nil {
		llog(l.LogE(err)).Error(
			"Error in reading the bytes from the origin file, skipping...")
		return
	}

	err = backlinkFile.Close()
	if err != nil {
		llog(l.LogE(err)).Error(
			"Error in closing the file after reading, moving on...")
		return
	}

	err = json.Unmarshal(byteBackLink, &backlink)
	if err != nil {
		llog(l.LogE(err)).Error(
			"Error in unmarshaling the files, skipping...")
		return
	}
	return
}

func SaveLayersBacklink(CVMFSRepo string, imgManifest da.Manifest, layerDigest []string) error {
	return SaveLayersBacklinkWithLogger(nil, CVMFSRepo, imgManifest, layerDigest)
}

func SaveLayersBacklinkWithLogger(logger *log.Entry, CVMFSRepo string, imgManifest da.Manifest, layerDigest []string) error {
	llog := func(entry *log.Entry) *log.Entry {
		return l.Ensure(entry).WithFields(log.Fields{"action": "save backlink",
			"repo": CVMFSRepo})
	}

	llog(logger).Trace("Start saving backlinks")

	backlinks := make(map[string][]byte)

	for _, layerDigest := range layerDigest {
		imgDigest := imgManifest.Config.Digest

		backlink, err := GetBacklinkFromLayer(CVMFSRepo, layerDigest)
		if err != nil {
			llog(l.LogE(err)).WithFields(log.Fields{"layer": layerDigest}).Error(
				"Error in obtaining the backlink from a layer digest, skipping...")
			continue
		}
		backlink.Origin = append(backlink.Origin, imgDigest)

		backlinkBytesMarshal, err := json.Marshal(backlink)
		if err != nil {
			llog(l.LogE(err)).WithFields(log.Fields{"layer": layerDigest}).Error(
				"Error in Marshaling back the files, skipping...")
			continue
		}

		backlinkPath := GetBacklinkPath(CVMFSRepo, layerDigest)
		backlinks[backlinkPath] = backlinkBytesMarshal
	}

	llog(logger).Trace("Start transaction")
	err := WithinTransactionWithLogger(logger, CVMFSRepo, func() error {

		for path, fileContent := range backlinks {
			// the path may not be there, check,
			// and if it doesn't exists create it
			dir := filepath.Dir(path)
			if _, err := os.Stat(dir); os.IsNotExist(err) {
				err = os.MkdirAll(dir, constants.DirPermision)
				if err != nil {
					llog(l.LogE(err)).WithFields(
						log.Fields{"file": path}).
						Error("Error in creating the directory for the backlinks file, skipping...")
					continue
				}
			}
			err := ioutil.WriteFile(path, fileContent, constants.FilePermision)
			if err != nil {
				llog(l.LogE(err)).WithFields(log.Fields{"file": path}).Error(
					"Error in writing the backlink file, skipping...")
				continue
			}
			llog(logger).WithFields(log.Fields{"file": path}).Trace(
				"Wrote backlink")
		}
		return nil
	})

	return err
}

func RemoveScheduleLocation(CVMFSRepo string) string {
	return filepath.Join("/", "cvmfs", CVMFSRepo, ".metadata", "remove-schedule.json")
}

func AddManifestToRemoveScheduler(CVMFSRepo string, manifest da.Manifest) error {
	return AddManifestToRemoveSchedulerWithLogger(nil, CVMFSRepo, manifest)
}

func AddManifestToRemoveSchedulerWithLogger(logger *log.Entry, CVMFSRepo string, manifest da.Manifest) error {
	schedulePath := RemoveScheduleLocation(CVMFSRepo)
	llog := func(entry *log.Entry) *log.Entry {
		return l.Ensure(entry).WithFields(log.Fields{
			"action": "add manifest to remove schedule",
			"file":   schedulePath})
	}
	var schedule []da.Manifest

	// if the file exist, load from it
	if _, err := os.Stat(schedulePath); !os.IsNotExist(err) {

		scheduleFileRO, err := os.OpenFile(schedulePath, os.O_RDONLY, constants.FilePermision)
		if err != nil {
			llog(l.LogE(err)).Error("Impossible to open the schedule file")
			return err
		}

		scheduleBytes, err := ioutil.ReadAll(scheduleFileRO)
		if err != nil {
			llog(l.LogE(err)).Error("Impossible to read the schedule file")
			return err
		}

		err = scheduleFileRO.Close()
		if err != nil {
			llog(l.LogE(err)).Error("Impossible to close the schedule file")
			return err
		}

		err = json.Unmarshal(scheduleBytes, &schedule)
		if err != nil {
			llog(l.LogE(err)).Error("Impossible to unmarshal the schedule file")
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

	err := WithinTransactionWithLogger(logger, CVMFSRepo, func() error {
		if _, err := os.Stat(schedulePath); os.IsNotExist(err) {
			err = os.MkdirAll(filepath.Dir(schedulePath), constants.DirPermision)
			if err != nil {
				llog(l.LogE(err)).Error("Error in creating the directory where save the schedule")
			}
		}

		bytes, err := json.Marshal(schedule)
		if err != nil {
			llog(l.LogE(err)).Error("Error in marshaling the new schedule")
		} else {

			err = ioutil.WriteFile(schedulePath, bytes, constants.FilePermision)
			if err != nil {
				llog(l.LogE(err)).Error("Error in writing the new schedule")
			} else {
				llog(logger).Trace("Wrote new remove schedule")
			}
		}
		return nil
	})

	return err
}

func RemoveSingularityImageFromManifest(CVMFSRepo string, manifest da.Manifest) error {
	dir := filepath.Join("/", "cvmfs", CVMFSRepo, manifest.GetSingularityPath())
	llog := func(l *log.Entry) *log.Entry {
		return l.WithFields(log.Fields{
			"action": "removing singularity directory", "directory": dir})
	}
	err := RemoveDirectory(CVMFSRepo, manifest.GetSingularityPath())
	if err != nil {
		llog(l.LogE(err)).Error("Error in removing singularity directory")
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

// from /cvmfs/$REPO/foo/bar -> foo/bar
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
		llog(l.LogE(err)).Error("Error in deleting a layer")
		return err
	}
	return nil
}

func RemoveDirectory(CVMFSRepo string, dirPath ...string) error {
	return RemoveDirectoryWithLogger(nil, CVMFSRepo, dirPath...)
}

func RemoveDirectoryWithLogger(logger *log.Entry, CVMFSRepo string, dirPath ...string) error {
	logger = l.Ensure(logger)
	path := []string{"/cvmfs", CVMFSRepo}
	for _, p := range dirPath {
		path = append(path, p)
	}
	directory := filepath.Join(path...)
	llog := func(entry *log.Entry) *log.Entry {
		return l.Ensure(entry).WithFields(log.Fields{
			"action": "removing directory", "directory": directory})
	}
	stat, err := os.Stat(directory)
	if err != nil {
		if os.IsNotExist(err) {
			llog(l.LogE(err)).Warning("Directory not existing")
			return nil
		}
		llog(l.LogE(err)).Error("Error in stating the directory")
		return err
	}
	if !stat.Mode().IsDir() {
		err = fmt.Errorf("Trying to remove something different from a directory")
		llog(l.LogE(err)).Error("Error, input is not a directory")
		return err
	}

	dirsSplitted := strings.Split(directory, string(os.PathSeparator))
	if len(dirsSplitted) <= 3 || dirsSplitted[1] != "cvmfs" {
		err := fmt.Errorf("Directory not in the CVMFS repo")
		llog(l.LogE(err)).Error("Error in opening the transaction")
		return err
	}
	err = WithinTransactionWithLogger(logger, CVMFSRepo, func() error {
		err := os.RemoveAll(directory)
		if err != nil {
			llog(l.LogE(err)).Error("Error in publishing after adding the backlinks")
		}
		return err
	})

	return err
}

func CreateCatalogIntoDir(CVMFSRepo, dir string) (err error) {
	return CreateCatalogIntoDirWithLogger(nil, CVMFSRepo, dir)
}

func CreateCatalogIntoDirWithLogger(logger *log.Entry, CVMFSRepo, dir string) (err error) {
	repoName, _ := GetRepoAndSubdir(CVMFSRepo)
	normalizedDir := PrefixRepoSubdirOnce(CVMFSRepo, dir)
	catalogRelativePath := filepath.Join(normalizedDir, ".cvmfscatalog")
	catalogPath := filepath.Join("/", "cvmfs", repoName, catalogRelativePath)
	if _, err := os.Stat(catalogPath); os.IsNotExist(err) {
		tmpFile, err := ioutil.TempFile("", "tempCatalog")
		if err != nil {
			return err
		}
		if err := tmpFile.Close(); err != nil {
			return err
		}
		err = PublishToCVMFSWithLogger(logger, repoName, catalogRelativePath, tmpFile.Name())
		if err != nil {
			return err
		}
		return err
	}
	return nil
}

// writes data to file and publish in cvmfs repo path
func WriteDataToCvmfs(CVMFSRepo, path string, data []byte) (err error) {
	return WriteDataToCvmfsWithLogger(nil, CVMFSRepo, path, data)
}

func WriteDataToCvmfsWithLogger(logger *log.Entry, CVMFSRepo, path string, data []byte) (err error) {
	logger = l.Ensure(logger)
	tmpFile, err := temp.UserDefinedTempFile()
	if err != nil {
		logger.WithField("error", err).Error("Error in creating temporary file for writing data")
		return err
	}
	defer os.RemoveAll(tmpFile.Name())
	err = ioutil.WriteFile(tmpFile.Name(), data, 0644)
	if err != nil {
		logger.WithField("error", err).Error("Error in writing data to file")
		return err
	}
	err = tmpFile.Close()
	if err != nil {
		logger.WithField("error", err).Error("Error in closing temporary file")
		return err
	}
	return PublishToCVMFSWithLogger(logger, CVMFSRepo, path, tmpFile.Name())
}

func removeHashMarkerIfPresent(digest string) string {
	t := strings.Split(digest, ":")
	if len(t) == 2 {
		return t[1]
	}
	return digest
}

// Overlay calls `cvmfs_server overlay` to merge multiple layer directories into
// a single flat image directory. The layers should be provided in bottom-to-top
// order (base layer first). The destPath is the path inside the CVMFS repository
// where the merged result will be placed (without the /cvmfs/$REPO prefix).
func Overlay(CVMFSRepo string, layerPaths []string, destPath string) error {
	return OverlayWithLogger(nil, CVMFSRepo, layerPaths, destPath, "", false)
}

func OverlayWithLogger(logger *log.Entry, CVMFSRepo string, layerPaths []string, destPath string, ociConfigPath string, skipSingularity bool) error {
	logger = l.Ensure(logger)
	if len(layerPaths) == 0 {
		return fmt.Errorf("no layer paths provided for overlay")
	}

	layers := strings.Join(layerPaths, ",")

	llog := func(entry *log.Entry) *log.Entry {
		return l.Ensure(entry).WithFields(log.Fields{
			"action": "overlay",
			"repo":   CVMFSRepo,
			"layers": layers,
			"dest":   destPath,
		})
	}

	llog(logger).Trace("Starting overlay merge")

	getLock(CVMFSRepo)
	defer unlock(CVMFSRepo)

	args := []string{"cvmfs_server", "overlay",
		"-l", layers,
		"-d", destPath}

	if ociConfigPath != "" {
		args = append(args, "-c", ociConfigPath)
	}
	if skipSingularity {
		args = append(args, "-S")
	}

	args = append(args, CVMFSRepo)

	err := exec.ExecCommandWithLogger(llog(logger), args...).Start()
	if err != nil {
		llog(l.LogE(err)).Error("Error in overlay merge")
		return err
	}

	llog(logger).Trace("Overlay merge complete")
	return nil
}
