package updater

import (
	"sync"

	"github.com/cvmfs/ducc/db"
	"github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
)

// TODO: Proper locking system for cvmfs. Need to look at existing code.
var cvmfsLock sync.Mutex

// TODO: Proper locking system for downloads.
var downloadsMutex = sync.Mutex{}
var pendingDownloads = make(map[digest.Digest]db.TaskPtr)
var useCount = make(map[digest.Digest]int)

func ManifestContainsForeignLayers(manifest v1.Manifest) bool {
	for _, layer := range manifest.Layers {
		if LayerMediaTypeIsForeign(layer.MediaType) {
			return true
		}
	}
	return false
}

// LayerMediaTypeIsValid returns true if the given layer media type is a valid OCI or Docker layer media type.
func LayerMediaTypeIsValid(layerMediaType string) bool {
	validLayerMediaTypes := []string{
		"application/vnd.oci.image.layer.v1.tar",
		"application/vnd.oci.image.layer.v1.tar+gzip",
		"application/vnd.oci.image.layer.nondistributable.v1.tar",
		"application/vnd.oci.image.layer.nondistributable.v1.tar+gzip",
		"application/vnd.docker.image.rootfs.diff.tar",
		"application/vnd.docker.image.rootfs.diff.tar.gzip",
		"application/vnd.docker.image.rootfs.foreign.diff.tar",
		"application/vnd.docker.image.rootfs.foreign.diff.tar.gzip",
	}
	for _, validLayerMediaType := range validLayerMediaTypes {
		if layerMediaType == validLayerMediaType {
			return true
		}
	}
	return false
}

// LayerMediaTypeIsCompressed returns true if the given layer media type is a gzip compressed layer.
func LayerMediaTypeIsCompressed(layerMediaType string) bool {
	compressedLayerMediaTypes := []string{
		"application/vnd.oci.image.layer.v1.tar+gzip",
		"application/vnd.oci.image.layer.nondistributable.v1.tar+gzip",
		"application/vnd.docker.image.rootfs.diff.tar.gzip",
		"application/vnd.docker.image.rootfs.foreign.diff.tar.gzip",
	}
	for _, compressedLayerMediaType := range compressedLayerMediaTypes {
		if layerMediaType == compressedLayerMediaType {
			return true
		}
	}
	return false
}

// LayerMediaTypeIsForeign returns true if the given layer media type is a foreign layer.
// Oci non-distributable layers are also considered foreign.
func LayerMediaTypeIsForeign(layerMediaType string) bool {
	foreignLayerTypes := []string{
		"application/vnd.oci.image.layer.nondistributable.v1.tar",
		"application/vnd.oci.image.layer.nondistributable.v1.tar+gzip",
		"application/vnd.docker.image.rootfs.foreign.diff.tar.gzip",
		"application/vnd.docker.image.rootfs.foreign.diff.tar",
	}
	for _, foreignLayerType := range foreignLayerTypes {
		if layerMediaType == foreignLayerType {
			return true
		}
	}
	return false
}
