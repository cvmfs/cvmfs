// Copyright CERN.
//
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

package singlemount

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	goexec "os/exec"
	"path"

	pb "github.com/cvmfs-contrib/cvmfs-csi/internal/cvmfs/singlemount/pb/v1"
	"github.com/cvmfs-contrib/cvmfs-csi/internal/exec"
	"github.com/cvmfs-contrib/cvmfs-csi/internal/log"
	"github.com/cvmfs-contrib/cvmfs-csi/internal/mountutils"
)

const (
	// Path to directory where the metadata and mountpoints are stored.
	// The structure is as follows:
	//
	//   <mountsDir>/
	//     targets.json
	//     <MountSingleRequest.MountId>/
	//       mount/
	//       bind.json
	//       config
	//       mount.json
	SinglemountsDir = "/var/lib/cvmfs.csi.cern.ch/single"

	// Contains mapping between all mountpoint -> mount ID that are currently
	// in use. We need to keep track of these, because CSI's NodeUnstageVolume
	// gives us only staging_target_path in the request, and we need a way to then
	// retrieve the mount ID to proceed with unmounting.
	mountpointsFilename = "mountpoints.json"

	// CVMFS mountpoint.
	mountpointDirname = "mount"

	// Mount metadata contains JSON-formatted mountMetadata
	// struct parsed from MountSingleRequest.
	mountMetadataFilename = "mount.json"

	// Bind metadata contains JSON-formatted bindMetadata struct.
	bindMetadataFilename = "bind.json"

	// CVMFS client config extracted from MountSingleRequest.
	cvmfsConfigFilename = "config"
)

type (
	mountMetadata struct {
		MountID    string
		Config     string
		Repository string
	}

	bindMetadata struct {
		Targets map[string]struct{}
	}

	mountpointsMetadata struct {
		Mountpoints map[string]string
	}
)

func fmtMountSingleBasePath(mountID string) string {
	return path.Join(SinglemountsDir, mountID)
}

func fmtMountpointPath(mountID string) string {
	return path.Join(fmtMountSingleBasePath(mountID), mountpointDirname)
}

func fmtMountMetadataPath(mountID string) string {
	return path.Join(fmtMountSingleBasePath(mountID), mountMetadataFilename)
}

func fmtBindMetadataPath(mountID string) string {
	return path.Join(fmtMountSingleBasePath(mountID), bindMetadataFilename)
}

func fmtConfigPath(mountID string) string {
	return path.Join(fmtMountSingleBasePath(mountID), cvmfsConfigFilename)
}

func fmtMountpointsMetadataPath() string {
	return path.Join(SinglemountsDir, mountpointsFilename)
}

// Creates the metadata directory for singlemount-runner.
// Must be called before RunBlocking().
// TOOD: make the path configurable and expose via the chart.
func CreateSingleMountsDir() error {
	return os.MkdirAll(SinglemountsDir, 0775)
}

// Makes sure that directory <mountsDir>/<MountSingleRequest.MountId> exists.
// If it doesn't, it is created and populated. If it already exists, it checks
// that the supplied MountSingleRequest matches metadata.json.
// Returns (true, nil) if this call created the singlemount metadata directory.
func ensureMountSingleMetadata(req *pb.MountSingleRequest) (bool, error) {
	if err := createMountSingleMetadata(req); err != nil {
		if os.IsExist(err) {
			return false, checkMountMetadataMatches(req)
		}

		return false, err
	}

	return true, nil
}

func writeConfigFile(mountID, config string) error {
	f, err := os.OpenFile(fmtConfigPath(mountID), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0444)
	if err != nil {
		return err
	}
	defer f.Close()

	// Write default values needed by cvmfs2.

	_, err = f.WriteString(fmt.Sprintf("CVMFS_RELOAD_SOCKETS=%s\n", fmtMountSingleBasePath(mountID)))
	if err != nil {
		return err
	}

	// Write the rest of the config.

	_, err = f.WriteString(config)
	if err != nil {
		return err
	}

	return nil
}

func createMountSingleMetadata(req *pb.MountSingleRequest) error {
	// Create the root singlemount directory.

	entryDir := fmtMountSingleBasePath(req.MountId)
	if err := os.Mkdir(entryDir, 0775); err != nil {
		return err
	}

	// Write mount metadata.

	mountMeta := mountMetadataFromMountSingleRequest(req)
	mountMetaJSON, err := json.Marshal(mountMeta)
	if err != nil {
		return err
	}

	err = os.WriteFile(path.Join(entryDir, mountMetadataFilename), mountMetaJSON, 0444)
	if err != nil {
		return err
	}

	// Write bind metadata.

	err = toJSONFile(
		path.Join(entryDir, bindMetadataFilename),
		bindMetadata{
			Targets: map[string]struct{}{
				req.Target: {},
			},
		},
	)
	if err != nil {
		return err
	}

	// Write CVMFS config.

	err = writeConfigFile(req.MountId, req.Config)
	if err != nil {
		return err
	}

	// Create mountpoint directory.

	if err = os.Mkdir(path.Join(entryDir, mountpointDirname), 0777); err != nil {
		return err
	}

	return nil
}

func checkMountMetadataMatches(req *pb.MountSingleRequest) error {
	if _, err := os.Stat(fmtMountSingleBasePath(req.MountId)); err != nil {
		if os.IsNotExist(err) {
			// Tha base directory doesn't exist, which means this is the
			// first request with such a mount ID, so there's nothing to check against.
			return nil
		}

		return fmt.Errorf("failed to stat mount metadata directory: %v", err)
	}

	storedMountMeta, err := fromJSONFile(fmtMountMetadataPath(req.MountId), mountMetadata{})
	if err != nil {
		return err
	}

	reqMountMeta := mountMetadataFromMountSingleRequest(req)

	checks := []struct {
		name     string
		expected string
		actual   string
	}{
		{"mountID", storedMountMeta.MountID, reqMountMeta.MountID},
		{"config", storedMountMeta.Config, reqMountMeta.Config},
		{"repository", storedMountMeta.Repository, reqMountMeta.Repository},
	}

	for _, c := range checks {
		if c.expected != c.actual {
			return fmt.Errorf("%s mismatch: expected %q, got %q", c.name, c.expected, c.actual)
		}
	}

	return nil
}

func mountMetadataFromMountSingleRequest(req *pb.MountSingleRequest) mountMetadata {
	return mountMetadata{
		MountID:    req.MountId,
		Config:     req.Config,
		Repository: req.Repository,
	}
}

// Adds MountSingleRequest.Target into bind metadata if it's missing.
func ensureBindMetadata(req *pb.MountSingleRequest) error {
	bindMetaFilepath := path.Join(fmtMountSingleBasePath(req.MountId), bindMetadataFilename)

	bindMeta, err := fromJSONFile(bindMetaFilepath, bindMetadata{})
	if err != nil {
		return err
	}

	if _, ok := bindMeta.Targets[req.Target]; ok {
		// Target is already listed in bindmounts, exit early.
		return nil
	}

	bindMeta.Targets[req.Target] = struct{}{}
	if err = toJSONFile(bindMetaFilepath, &bindMeta); err != nil {
		return err
	}

	return nil
}

// Delete UnmountSingleRequest.Mountpoint from bind metadata if it's present.
// Returns (true, nil) if there are no more bindmounts listed in metadata.
// This then means the CVMFS mount itself can be unmounted, and the singlemount dir
// can be removed.
func deleteBindMetadata(req *pb.UnmountSingleRequest, mountID string) (bool, error) {
	bindMetaFilepath := path.Join(fmtMountSingleBasePath(mountID), bindMetadataFilename)

	bindMeta, err := fromJSONFile(bindMetaFilepath, bindMetadata{})
	if err != nil {
		return false, err
	}

	if _, ok := bindMeta.Targets[req.Mountpoint]; !ok {
		// Mountpoint is missing in bindmounts, assume it was already deleted.
		return false, nil
	}

	delete(bindMeta.Targets, req.Mountpoint)
	if err = toJSONFile(bindMetaFilepath, &bindMeta); err != nil {
		return false, err
	}

	return len(bindMeta.Targets) == 0, nil
}

func makeSharedMount(req *pb.MountSingleRequest) error {
	_, err := ensureMountSingleMetadata(req)
	if err != nil {
		return nil
	}

	// Clean up after ensureMountSingleMetadata().
	defer ifErr(
		&err,
		func() {
			if err2 := os.RemoveAll(fmtMountSingleBasePath(req.MountId)); err2 != nil {
				log.Errorf("failed to clean up singlemount directory %s: %v",
					fmtMountSingleBasePath(req.MountId), err)
			}
		},
	)

	err = tryMountOrRecover(
		&cvmfsMounterUnmounter{
			repository: req.Repository,
			configPath: fmtConfigPath(req.MountId),
		},
		fmtMountpointPath(req.MountId),
	)
	if err != nil {
		return err
	}

	// Clean up after tryMountOrRecover(cvmfsMounterUnmounter).
	defer ifErr(
		&err,
		func() {
			err2 := cvmfsMounterUnmounter{}.unmount(fmtMountpointPath(req.MountId))
			if err2 != nil {
				log.Errorf("failed to clean up cvmfs2 mount %s: %v",
					fmtMountpointPath(req.MountId), err)
			}
		},
	)

	err = tryMountOrRecover(
		&bindMounterUnmounter{
			cvmfsMountpoint: fmtMountpointPath(req.MountId),
		},
		req.Target,
	)
	if err != nil {
		return err
	}

	// Clean up after tryMountOrRecover(bindMounterUnmounter).
	defer ifErr(
		&err,
		func() {
			err2 := bindMounterUnmounter{}.unmount(req.Target)
			if err2 != nil {
				log.Errorf("failed to clean up bind mount %s: %v",
					req.Target, err)
			}
		},
	)

	return nil
}

func tryMountOrRecover(mu mounterUnmounter, mountpointPath string) error {
	mntState, err := mountutils.GetState(mountpointPath)
	if err != nil {
		return err
	}

	switch mntState {
	case mountutils.StMounted:
		return nil
	case mountutils.StCorrupted:
		if err = mu.unmount(mountpointPath); err != nil {
			return err
		}
		fallthrough
	case mountutils.StNotMounted:
		return mu.mount(mountpointPath)
	default:
		return fmt.Errorf("mountpoint %s is in unexpected state", mountpointPath)
	}
}

type (
	mounterUnmounter interface {
		mount(mountpoint string) error
		unmount(mounpoint string) error
	}

	cvmfsMounterUnmounter struct {
		mounterUnmounter
		repository string
		configPath string
	}

	bindMounterUnmounter struct {
		mounterUnmounter
		cvmfsMountpoint string
	}
)

func (mu cvmfsMounterUnmounter) mount(mountpoint string) error {
	cvmfsArgs := []string{
		mu.repository,
		mountpoint,
		"-o", fmt.Sprintf("config=%s", mu.configPath),
	}

	if log.LevelEnabled(log.LevelTrace) {
		cvmfsArgs = append(cvmfsArgs, "-d")
	}

	return runCvmfs2AndTryCaptureErr(cvmfsArgs...)
}

func (mu cvmfsMounterUnmounter) unmount(mountpoint string) error {
	out, err := exec.CombinedOutput(goexec.Command("fusermount", "-u", mountpoint))
	if err != nil {
		// Ignore these errors for idempotency:
		// * Not a mountpoint: ": Invalid argument"
		// * ENOENT: ": No such file or directory"
		if bytes.Contains(out, []byte(": Invalid argument")) ||
			bytes.Contains(out, []byte(": No such file or directory")) {
			return nil
		}

		err = fmt.Errorf("failed to unmount %s: output: %s; error: %v", mountpoint, out, err)
	}

	return err
}

func (mu bindMounterUnmounter) mount(mountpoint string) error {
	out, err := exec.CombinedOutput(goexec.Command(
		"mount",
		"--bind",
		mu.cvmfsMountpoint,
		mountpoint,
	))

	if err != nil {
		log.Errorf("failed to bind %s to %s: output: %s; error: %v", mu.cvmfsMountpoint, mountpoint, out, err)
	}

	return err

}

func (mu bindMounterUnmounter) unmount(mountpoint string) error {
	out, err := exec.CombinedOutput(goexec.Command("umount", mountpoint))
	if err != nil {
		// Ignore these errors for idempotency:
		// * Not a mountpoint: ": not mounted"
		// * ENOENT: ": no mount point specified"
		if bytes.Contains(out, []byte(": not mounted")) ||
			bytes.Contains(out, []byte(": no mount point specified")) {
			return nil
		}

		err = fmt.Errorf("failed to unmount bindmount %s: output: %s; error: %v", mountpoint, out, err)
	}

	return err

}

func addMountpointMetadata(mountpoint, mountID string) error {
	mountpointsMeta, err := fromJSONFile(fmtMountpointsMetadataPath(), mountpointsMetadata{
		Mountpoints: make(map[string]string),
	})
	if err != nil {
		return err
	}

	if storedMountID, ok := mountpointsMeta.Mountpoints[mountpoint]; ok {
		if storedMountID == mountID {
			// Mountpoint already registered with this mount ID.
			return nil
		}

		return fmt.Errorf(
			"failed to register mountpoint %s with mount ID %s: mountpoint already exists with mount ID %s",
			mountpoint, mountID, storedMountID,
		)
	}

	mountpointsMeta.Mountpoints[mountpoint] = mountID

	if err = toJSONFile(fmtMountpointsMetadataPath(), &mountpointsMeta); err != nil {
		return err
	}

	return nil
}

func deleteMountpointMetadata(mountpoint string) error {
	mountpointsMeta, err := fromJSONFile(fmtMountpointsMetadataPath(), mountpointsMetadata{
		Mountpoints: make(map[string]string),
	})
	if err != nil {
		return err
	}

	if _, ok := mountpointsMeta.Mountpoints[mountpoint]; !ok {
		// Mountpoint not found in map, assume it was already deleted.
		return nil
	}

	delete(mountpointsMeta.Mountpoints, mountpoint)

	if err = toJSONFile(fmtMountpointsMetadataPath(), &mountpointsMeta); err != nil {
		return err
	}

	return nil
}

func getMountIDForMountpoint(mountpoint string) (string, error) {
	mountpointsMeta, err := fromJSONFile(fmtMountpointsMetadataPath(), mountpointsMetadata{
		Mountpoints: make(map[string]string),
	})
	if err != nil {
		return "", err
	}

	return mountpointsMeta.Mountpoints[mountpoint], nil
}
