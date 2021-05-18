package cvmfs

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"

	"github.com/containerd/containerd/log"
	"github.com/containerd/stargz-snapshotter/snapshot"
)

const (
	// targetRefLabelCRI is a label which contains image reference passed from CRI plugin
	targetRefLabelCRI = "containerd.io/snapshot/cri.image-ref"
	// targetDigestLabelCRI is a label which contains layer digest passed from CRI plugin
	targetDigestLabelCRI = "containerd.io/snapshot/cri.layer-digest"
	// targetImageLayersLabel is a label which contains layer digests contained in
	// the target image and is passed from CRI plugin.
	targetImageLayersLabel = "containerd.io/snapshot/cri.image-layers"
)

type Filesystem struct {
	fsAbsoluteMountpoint string
	mountedLayers        map[string]string
	mountedLayersLock    sync.Mutex
}

type Config struct {
	Repository         string `toml:"repository" default:"unpacked.cern.ch"`
	AbsoluteMountpoint string `toml:"absolute-mountpoint" default:""`
}

func NewFilesystem(ctx context.Context, root string, config *Config) (snapshot.FileSystem, error) {
	var absolutePath string
	mountedLayersMap := make(map[string]string)
	if config.AbsoluteMountpoint == "" {
		repository := config.Repository
		if repository == "" {
			repository = "unpacked.cern.ch"
		}
		absolutePath = filepath.Join("/", "cvmfs", repository)
	} else {
		absolutePath = config.AbsoluteMountpoint
	}
	log.G(ctx).WithField("root", root).WithField("absolutePath", absolutePath).Info("Mounting new filesystem")
	if _, err := os.Stat(absolutePath); err != nil {
		log.G(ctx).WithField("absolutePath", absolutePath).Warning("Impossible to stat the absolute path, is the filesystem mounted properly? Error: ", err)
	}
	return &Filesystem{fsAbsoluteMountpoint: absolutePath, mountedLayers: mountedLayersMap}, nil
}

func (fs *Filesystem) Mount(ctx context.Context, mountpoint string, labels map[string]string) error {
	log.G(ctx).Info("Mount layer from cvmfs")
	digest, ok := labels[targetDigestLabelCRI]
	if !ok {
		err := fmt.Errorf("cvmfs: layer digest has not be passed")
		log.G(ctx).Debug(err.Error())
		return err
	}
	digest = strings.Split(digest, ":")[1]
	firstTwo := digest[0:2]
	path := filepath.Join(fs.fsAbsoluteMountpoint, ".layers", firstTwo, digest, "layerfs")
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
	fs.mountedLayersLock.Lock()
	defer fs.mountedLayersLock.Unlock()
	fs.mountedLayers[mountpoint] = path
	return nil
}

func (fs *Filesystem) Check(ctx context.Context, mountpoint string, labels map[string]string) error {
	log.G(ctx).WithField("snapshotter", "cvmfs").WithField("mountpoint", mountpoint).Warning("checking layer")
	fs.mountedLayersLock.Lock()
	path, ok := fs.mountedLayers[mountpoint]
	fs.mountedLayersLock.Unlock()
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

func (fs *Filesystem) Unmount(ctx context.Context, mountpoint string) error {
	// maybe we lost track of something somehow, does not hurt to try to unmount the mountpoint anyway

	fs.mountedLayersLock.Lock()
	_, ok := fs.mountedLayers[mountpoint]
	delete(fs.mountedLayers, mountpoint)
	fs.mountedLayersLock.Unlock()

	if !ok {
		err := fmt.Errorf("Trying to unmount mountpoint that does not seems mounted: %s", mountpoint)
		log.G(ctx).WithError(err).Error("Layer does not seems mounted.")
	}
	return syscall.Unmount(mountpoint, syscall.MNT_FORCE)
}

func (fs *Filesystem) UnmountAll(ctx context.Context) {
	log.G(ctx).Info("Unmounting all the layers")
	m := make([]string, 0)
	fs.mountedLayersLock.Lock()
	for mountpoint := range fs.mountedLayers {
		m = append(m, mountpoint)
		delete(fs.mountedLayers, mountpoint)
	}
	fs.mountedLayersLock.Unlock()
	log.G(ctx).WithField("layers", m).Info("Unmounting the layers")
	for _, mountpoint := range m {
		log.G(ctx).WithField("layer", mountpoint).Info("Unmounting the layer")
		if err := syscall.Unmount(mountpoint, syscall.MNT_FORCE); err != nil {
			log.G(context.TODO()).WithError(err).WithField("mountpoint", mountpoint).Error("Error in unmounting before to exit")
		}
	}
}
