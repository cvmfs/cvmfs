package updater

import (
	"fmt"
	"sync"

	"github.com/cvmfs/ducc/db"
	"github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
)

// TODO: Proper locking system for cvmfs. Need to look at existing code.
var cvmfsLock sync.RWMutex

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

func FullUpdate(image db.Image, manifest ManifestWithBytesAndDigest, outputOptions db.WishOutputOptions, cvmfsRepo string) (db.TaskPtr, error) {
	task, ptr, err := db.CreateTask(nil, db.TASK_UPDATE)
	if err != nil {
		return db.TaskPtr{}, err
	}

	// Sanity check options
	if outputOptions.CreatePodman.Value && !outputOptions.CreateLayers.Value {
		task.LogFatal(nil, "Cannot create podman output without creating layers")
		return ptr, err
	}
	if outputOptions.CreateThinImage.Value && !outputOptions.CreateLayers.Value {
		task.LogFatal(nil, "Cannot create thin image output without creating layers")
		return ptr, err
	}

	// Create the output tasks
	var (
		createFlatTask   db.TaskPtr
		createLayersTask db.TaskPtr
		createPodmanTask db.TaskPtr
		//createThinTask   db.TaskPtr
	)

	if outputOptions.CreateFlat.Value {
		createFlatTask, err = CreateFlat(image, manifest, cvmfsRepo)
		if err != nil {
			task.LogFatal(nil, fmt.Sprintf("Failed to create create flat task: %s", err.Error()))
			return ptr, err
		}
		if err := task.LinkSubtask(nil, createFlatTask); err != nil {
			task.LogFatal(nil, fmt.Sprintf("Failed to link create flat task: %s", err.Error()))
			return ptr, err
		}
	}

	if outputOptions.CreateLayers.Value {
		createLayersTask, err = CreateLayers(image, manifest, cvmfsRepo)
		if err != nil {
			task.LogFatal(nil, fmt.Sprintf("Failed to create create layers task: %s", err.Error()))
			return ptr, err
		}
		if err := task.LinkSubtask(nil, createLayersTask); err != nil {
			task.LogFatal(nil, fmt.Sprintf("Failed to link create layers task: %s", err.Error()))
			return ptr, err
		}
	}

	if outputOptions.CreatePodman.Value {
		createPodmanTask, err = CreatePodman(image, manifest, cvmfsRepo)
		if err != nil {
			task.LogFatal(nil, fmt.Sprintf("Failed to create create podman task: %s", err.Error()))
			return ptr, err
		}
		if err := task.LinkSubtask(nil, createPodmanTask); err != nil {
			task.LogFatal(nil, fmt.Sprintf("Failed to link create podman task: %s", err.Error()))
			return ptr, err
		}
	}

	/*if outputOptions.CreateThinImage.Value {
		createThinTask, err = CreateThinImage(image, manifest, cvmfsRepo)
		if err != nil {
			task.LogFatal(nil, fmt.Sprintf("Failed to create create thin image task: %s", err.Error()))
			return ptr, err
		}
		if err := task.LinkSubtask(nil, createThinTask); err != nil{
			task.LogFatal(nil, fmt.Sprintf("Failed to link create thin image task: %s", err.Error()))
			return ptr, err
		}
	}*/

	go func() {
		task.Log(nil, db.LOG_SEVERITY_DEBUG, "Waiting for start")
		task.WaitForStart()
		task.Log(nil, db.LOG_SEVERITY_DEBUG, "Starting update")

	}()

	return ptr, nil
}
