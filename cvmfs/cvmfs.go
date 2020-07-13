package cvmfs

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/containerd/containerd/log"
	"github.com/containerd/stargz-snapshotter/snapshot"
	"github.com/containerd/stargz-snapshotter/stargz/handler"
)

type filesystem struct {
	repository    string
	mountedLayers map[string]string
}

type Config struct {
	Repository string `toml:"repository" default:"unpacked.cern.ch"`
}

func NewFilesystem(ctx context.Context, root string, config *Config) (snapshot.FileSystem, error) {
	log.G(ctx).WithField("root", root).Warning("New fs")
	repository := config.Repository
	if repository == "" {
		repository = "unpacked.cern.ch"
	}
	return &filesystem{repository: repository, mountedLayers: make(map[string]string)}, nil
}

func (fs *filesystem) Mount(ctx context.Context, mountpoint string, labels map[string]string) error {
	log.G(ctx).Warning("Mount: cvmfs")
	digest, ok := labels[handler.TargetDigestLabel]
	if !ok {
		err := fmt.Errorf("cvmfs: digest hasn't be passed")
		log.G(ctx).Debug(err.Error())
		return err
	}
	digest = strings.Split(digest, ":")[1]
	firstTwo := digest[0:2]
	path := filepath.Join("/", "cvmfs", fs.repository, ".layers", firstTwo, digest, "layerfs")
	if _, err := os.Stat(path); os.IsNotExist(err) {
		err = fmt.Errorf("layer %s not in the cvmfs repository", digest)
		log.G(ctx).WithError(err).WithField("layer digest", digest).WithField("path", path).Debug("cvmfs: Layer not found")
		return err
	}
	log.G(ctx).WithField("layer digest", digest).Debug("cvmfs: Layer present in CVMFS")
	err := syscall.Mount(path, mountpoint, "", syscall.MS_BIND, "")
	if err != nil {
		log.G(ctx).WithError(err).WithField("layer digest", digest).WithField("mountpoint", mountpoint).Debug("cvmfs: Error in bind mounting the layer.")
		return err
	}
	fs.mountedLayers[mountpoint] = path
	return nil
}

func (fs *filesystem) Check(ctx context.Context, mountpoint string) error {
	log.G(ctx).WithField("snapshotter", "cvmfs").WithField("mountpoint", mountpoint).Warning("checking layer")
	path, ok := fs.mountedLayers[mountpoint]
	if !ok {
		err := fmt.Errorf("Mountpoint: %s was not mounted", mountpoint)
		log.G(ctx).WithError(err).WithField("mountpoint", mountpoint).Error("cvmfs: the requested mountpoint does not seem to be mounted")
		return err
	}

	_, statErr := os.Stat(path)
	if statErr == nil {
		return nil
	}
	if statErr != nil {
		if os.IsNotExist(statErr) {
			err := fmt.Errorf("Layer from path: %s does not seems to be in the CVMFS repository", path)
			log.G(ctx).WithError(err).WithField("mountpoint", mountpoint).WithField("layer path", path).Error("cvmfs: the mounted layer does not seem to exist.")
			return err
		}
		err := fmt.Errorf("Error in stat-ing the layer: %s", statErr)
		log.G(ctx).WithError(err).WithField("mountpoint", mountpoint).WithField("layer path", path).Error("cvmfs: unknow error in stating the file.")
		return err
	}
	return statErr
}
