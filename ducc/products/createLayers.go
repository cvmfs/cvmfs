package products

import (
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"sync"

	"github.com/cvmfs/ducc/config"
	"github.com/cvmfs/ducc/constants"
	"github.com/cvmfs/ducc/cvmfs"
	"github.com/cvmfs/ducc/db"
	"github.com/cvmfs/ducc/lib"
	"github.com/cvmfs/ducc/registry"
	"github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
)

type ingestLayerKey struct {
	LayerDigest digest.Digest
	cvmfsRepo   string
}

var currentlyIngestingLayersMutex = sync.Mutex{}
var currentlyIngestingLayers = make(map[ingestLayerKey]db.TaskPtr)

func CreateLayers(image db.Image, manifest registry.ManifestWithBytesAndDigest, cvmfsRepo string) (db.TaskPtr, error) {
	titleStr := fmt.Sprintf("Create layers for %s in %s", image.GetSimpleName(), cvmfsRepo)
	task, ptr, err := db.CreateTask(nil, db.TASK_CREATE_LAYERS, titleStr)
	if err != nil {
		return db.NullTaskPtr(), err
	}

	// Ingest the layers
	ingestLayersTask, err := ingestLayers(image, manifest.Manifest, cvmfsRepo)
	if err != nil {
		task.LogFatal(nil, fmt.Sprintf("Failed to create task of type %s: %s", db.TASK_INGEST_LAYERS, err.Error()))
		return ptr, nil
	}
	if err := task.LinkSubtask(nil, ingestLayersTask); err != nil {
		task.LogFatal(nil, fmt.Sprintf("Failed to add \"%s\" as subtask: %s", db.TASK_INGEST_LAYERS, err.Error()))
		return ptr, nil
	}

	writeImageMetadataTask, err := writeImageMetadata(image, manifest, cvmfsRepo)
	if err != nil {
		task.LogFatal(nil, fmt.Sprintf("Failed to create task of type %s: %s", db.TASK_WRITE_IMAGE_METADATA, err.Error()))
		return ptr, nil
	}
	if err := task.LinkSubtask(nil, writeImageMetadataTask); err != nil {
		task.LogFatal(nil, fmt.Sprintf("Failed to add \"%s\" as subtask: %s", db.TASK_WRITE_IMAGE_METADATA, err.Error()))
		return ptr, nil
	}

	go func() {
		task.Log(nil, db.LOG_SEVERITY_DEBUG, "Waiting for task to start")
		task.WaitForStart()
		task.Log(nil, db.LOG_SEVERITY_INFO, "Task started")

		// Start ingesting the layers
		ingestLayersTask.Start(nil)
		task.Log(nil, db.LOG_SEVERITY_INFO, "Started task for ingesting layers. Waiting for it to finish")
		ingestLayersResult := ingestLayersTask.WaitUntilDone()
		if !db.TaskResultSuccessful(ingestLayersResult) {
			task.LogFatal(nil, "Failed to ingest layers")
			writeImageMetadataTask.Cancel(nil)
			return
		}
		task.Log(nil, db.LOG_SEVERITY_INFO, "Successfully ingested layers")

		// Start writing the image metadata
		writeImageMetadataTask.Start(nil)
		task.Log(nil, db.LOG_SEVERITY_INFO, "Started task for writing image metadata. Waiting for it to finish")
		writeImageMetadataResult := writeImageMetadataTask.WaitUntilDone()
		if !db.TaskResultSuccessful(writeImageMetadataResult) {
			task.LogFatal(nil, "Failed to write image metadata")
			return
		}
		task.Log(nil, db.LOG_SEVERITY_INFO, "Successfully wrote image metadata")

		// Mark the task as completed
		task.SetTaskCompleted(nil, db.TASK_RESULT_SUCCESS)
		task.Log(nil, db.LOG_SEVERITY_INFO, "Task completed")
	}()

	return ptr, nil
}

func ingestLayers(image db.Image, manifest v1.Manifest, cvmfsRepo string) (db.TaskPtr, error) {
	titleStr := fmt.Sprintf("Ingest layers for %s to %s", image.GetSimpleName(), cvmfsRepo)
	task, ptr, err := db.CreateTask(nil, db.TASK_INGEST_LAYERS, titleStr)
	if err != nil {
		return db.NullTaskPtr(), err
	}

	createLayerTasks := make([]db.TaskPtr, len(manifest.Layers))
	for i, layer := range manifest.Layers {
		isCompressed := LayerMediaTypeIsCompressed(layer.MediaType)
		createLayerTask, err := createLayer(image, layer.Digest, isCompressed, cvmfsRepo)
		if err != nil {
			task.LogFatal(nil, fmt.Sprintf("Failed to create task of type %s: %s", db.TASK_CREATE_LAYER, err.Error()))
			return ptr, nil
		}
		if err := task.LinkSubtask(nil, createLayerTask); err != nil {
			task.LogFatal(nil, fmt.Sprintf("Failed to add \"%s\" as subtask: %s", db.TASK_CREATE_LAYER, err.Error()))
			return ptr, nil
		}
		createLayerTasks[i] = createLayerTask
	}

	go func() {
		task.Log(nil, db.LOG_SEVERITY_DEBUG, "Waiting for task to start")
		task.WaitForStart()
		task.Log(nil, db.LOG_SEVERITY_INFO, "Task started")

		// Start creating the layers
		for _, createLayerTask := range createLayerTasks {
			createLayerTask.Start(nil)
		}
		task.Log(nil, db.LOG_SEVERITY_INFO, "Started tasks for creating layers. Waiting for them to finish")
		for _, createLayerTask := range createLayerTasks {
			createLayerResult := createLayerTask.WaitUntilDone()
			if !db.TaskResultSuccessful(createLayerResult) {
				task.LogFatal(nil, "Not all layers were ingested successfully")
				return
			}
		}
		task.Log(nil, db.LOG_SEVERITY_INFO, "Successfully ingested layers")
		task.SetTaskCompleted(nil, db.TASK_RESULT_SUCCESS)
		task.Log(nil, db.LOG_SEVERITY_INFO, "Task completed")
	}()
	return ptr, nil
}

func createLayer(image db.Image, layerDigest digest.Digest, compressed bool, cvmfsRepo string) (db.TaskPtr, error) {
	// Check if we are already creating this layer
	currentlyIngestingLayersMutex.Lock()
	if ptr, ok := currentlyIngestingLayers[ingestLayerKey{layerDigest, cvmfsRepo}]; ok {
		currentlyIngestingLayersMutex.Unlock()
		return ptr, nil
	}
	titleStr := fmt.Sprintf("Create layer %s for %s in %s", layerDigest.String(), image.GetSimpleName(), cvmfsRepo)
	task, ptr, err := db.CreateTask(nil, db.TASK_CREATE_LAYER, titleStr)
	if err != nil {
		currentlyIngestingLayersMutex.Unlock()
		return db.NullTaskPtr(), err
	}
	// Register that we are creating this layer
	currentlyIngestingLayers[ingestLayerKey{layerDigest, cvmfsRepo}] = ptr
	currentlyIngestingLayersMutex.Unlock()

	// If the layer is already in cvmfs, we can skip it and return instantly
	exists, err := layerExistsInCvmfs(layerDigest, cvmfsRepo)
	if err != nil {
		task.LogFatal(nil, fmt.Sprintf("Failed to check if layer %s exists in cvmfs: %s", layerDigest.String(), err.Error()))
		return ptr, nil
	}
	if exists {
		task.Log(nil, db.LOG_SEVERITY_INFO, fmt.Sprintf("Layer %s already exists in cvmfs", layerDigest.String()))
		task.SetTaskCompleted(nil, db.TASK_RESULT_SKIPPED)
		return ptr, nil
	}

	// We create a task to download the layer
	registryPtr := registry.GetOrCreateRegistry(registry.ContainerRegistryIdentifier{Scheme: image.RegistryScheme, Hostname: image.RegistryHost})
	downloadLayerTask, err := DownloadBlob(registryPtr, image.Repository, layerDigest, nil)
	if err != nil {
		task.LogFatal(nil, fmt.Sprintf("Failed to create task of type %s: %s", db.TASK_DOWNLOAD_BLOB, err.Error()))
		return ptr, nil
	}
	// Want to release the blob as soon as we are done with it.
	// If we return early, we need to release it in this function.
	// If not, the task goroutine will release it.
	earlyReturn := true
	defer func() {
		if earlyReturn {
			releaseBlob(layerDigest)
		}
	}()
	if err := task.LinkSubtask(nil, downloadLayerTask); err != nil {
		task.LogFatal(nil, fmt.Sprintf("Failed to add \"%s\" as subtask: %s", db.TASK_DOWNLOAD_BLOB, err.Error()))
		return ptr, nil
	}

	// We create a task to ingest the layer
	ingestLayerTask, err := ingestLayer(layerDigest, compressed, cvmfsRepo)
	if err != nil {
		task.LogFatal(nil, fmt.Sprintf("Failed to create task of type %s: %s", db.TASK_INGEST_LAYER, err.Error()))
		return ptr, nil
	}
	if err := task.LinkSubtask(nil, ingestLayerTask); err != nil {
		task.LogFatal(nil, fmt.Sprintf("Failed to add \"%s\" as subtask: %s", db.TASK_INGEST_LAYER, err.Error()))
		return ptr, nil
	}

	earlyReturn = false
	go func() {
		defer releaseBlob(layerDigest)
		task.Log(nil, db.LOG_SEVERITY_DEBUG, "Waiting for task to start")
		task.WaitForStart()
		task.Log(nil, db.LOG_SEVERITY_INFO, "Task started")

		// Start downloading the layer
		downloadLayerTask.Start(nil)
		task.Log(nil, db.LOG_SEVERITY_INFO, "Started task for downloading layer. Waiting for it to finish")
		downloadLayerResult := downloadLayerTask.WaitUntilDone()
		if !db.TaskResultSuccessful(downloadLayerResult) {
			task.LogFatal(nil, "Failed to download layer")
			return
		}
		task.Log(nil, db.LOG_SEVERITY_INFO, "Successfully downloaded layer")

		// Start ingesting the layer
		ingestLayerTask.Start(nil)
		task.Log(nil, db.LOG_SEVERITY_INFO, "Started task for ingesting layer. Waiting for it to finish")
		ingestLayerResult := ingestLayerTask.WaitUntilDone()
		if !db.TaskResultSuccessful(ingestLayerResult) {
			task.LogFatal(nil, "Failed to ingest layer")
			return
		}
		task.Log(nil, db.LOG_SEVERITY_INFO, "Successfully created layer")

		// Remove the layer from the map of currently ingesting layers
		currentlyIngestingLayersMutex.Lock()
		delete(currentlyIngestingLayers, ingestLayerKey{layerDigest, cvmfsRepo})
		currentlyIngestingLayersMutex.Unlock()

		// Mark the task as completed
		task.SetTaskCompleted(nil, db.TASK_RESULT_SUCCESS)
		task.Log(nil, db.LOG_SEVERITY_INFO, "Task completed")
	}()

	return ptr, nil
}

func ingestLayer(layerDigest digest.Digest, compressed bool, cvmfsRepo string) (db.TaskPtr, error) {
	titleStr := fmt.Sprintf("Ingest layer %s to %s", layerDigest.String(), cvmfsRepo)
	task, ptr, err := db.CreateTask(nil, db.TASK_INGEST_LAYER, titleStr)
	if err != nil {
		return db.NullTaskPtr(), err
	}

	go func() {
		task.Log(nil, db.LOG_SEVERITY_DEBUG, "Waiting for task to start")
		task.WaitForStart()
		task.Log(nil, db.LOG_SEVERITY_INFO, "Task started")

		blobPath := path.Join(config.TMP_FILE_PATH, "blobs", layerDigest.Encoded())
		fileReader, err := os.Open(blobPath)
		if err != nil {
			task.LogFatal(nil, fmt.Sprintf("Failed to open layer file %s: %s", blobPath, err.Error()))
			return
		}
		defer fileReader.Close()
		task.Log(nil, db.LOG_SEVERITY_DEBUG, fmt.Sprintf("Opened layer file %s", blobPath))
		var uncompressedReader io.ReadCloser
		if compressed {
			// Compressed layers need to be decompressed
			gzipReader, err := gzip.NewReader(fileReader)
			if err != nil {
				task.LogFatal(nil, fmt.Sprintf("Failed to create gzip reader for layer file %s: %s", blobPath, err))
				return
			}
			uncompressedReader = gzipReader
			task.Log(nil, db.LOG_SEVERITY_DEBUG, "Layer is compressed, adding gzip reader for decompression")
		} else {
			uncompressedReader = fileReader
			task.Log(nil, db.LOG_SEVERITY_DEBUG, "Layer is not compressed, using file reader directly")
		}
		// We get the digest and size of the uncompressed layer
		readHashCloseSizer := lib.NewReadAndHash(uncompressedReader)

		task.Log(nil, db.LOG_SEVERITY_INFO, "Waiting for lock on cvmfs repo")
		cvmfsLock.Lock()
		defer cvmfsLock.Unlock()

		// BEGIN CVMFS METATRANSACTION
		// TODO: This should be done in a transaction, as failing in the middle would
		// leave the repository in an inconsistent state.

		task.Log(nil, db.LOG_SEVERITY_DEBUG, "Starting CVMFS transaction for creating catalogs")
		layerPath := cvmfs.TrimCVMFSRepoPrefix(cvmfs.LayerPath(cvmfsRepo, layerDigest.Encoded()))
		if err := cvmfs.CreateCatalogIntoDir(cvmfsRepo, layerPath); err != nil {
			cvmfs.RemoveDirectory(cvmfsRepo, layerPath)
			task.LogFatal(nil, fmt.Sprintf("Failed to create catalog for layer %s: %s", layerDigest.String(), err.Error()))
			return
		}
		task.Log(nil, db.LOG_SEVERITY_DEBUG, fmt.Sprintf("Created catalog for layer %s", layerDigest.String()))

		// Ingest the layer FS
		task.Log(nil, db.LOG_SEVERITY_DEBUG, "Starting CVMFS for ingesting layer")
		ingestPath := cvmfs.TrimCVMFSRepoPrefix(cvmfs.LayerRootfsPath(cvmfsRepo, layerDigest.Encoded()))
		if err := cvmfs.Ingest(cvmfsRepo, readHashCloseSizer, "--catalog", "-t", "-", "-b", ingestPath); err != nil {
			cvmfs.RemoveDirectory(cvmfsRepo, layerPath)
			task.LogFatal(nil, fmt.Sprintf("Failed to ingest layer %s: %s", layerDigest.String(), err.Error()))
			return
		}
		task.Log(nil, db.LOG_SEVERITY_DEBUG, fmt.Sprintf("Successfully ingested layer %s", layerDigest.String()))

		// Write the layer metadata
		task.Log(nil, db.LOG_SEVERITY_DEBUG, "Starting CVMFS transaction for writing metadata")
		if err := lib.StoreLayerInfo(cvmfsRepo, layerDigest.Encoded(), readHashCloseSizer); err != nil {
			cvmfs.RemoveDirectory(cvmfsRepo, layerPath)
			task.LogFatal(nil, fmt.Sprintf("Failed to store layer metadata for layer %s: %s", layerDigest.String(), err.Error()))
			return
		}
		task.Log(nil, db.LOG_SEVERITY_DEBUG, fmt.Sprintf("Successfully stored layer metadata for layer %s", layerDigest.String()))
		// END CVMFS METATRANSACTION

		task.Log(nil, db.LOG_SEVERITY_DEBUG, fmt.Sprintf("Released blob %s", layerDigest.String()))

		// Mark the task as completed
		task.SetTaskCompleted(nil, db.TASK_RESULT_SUCCESS)
		task.Log(nil, db.LOG_SEVERITY_INFO, "Task completed")
	}()

	return ptr, nil
}

func layerExistsInCvmfs(layerDigest digest.Digest, cvmfsRepo string) (bool, error) {
	// Check if the layer is already in cvmfs
	_, err := os.Stat(cvmfs.LayerPath(cvmfsRepo, layerDigest.Encoded()))
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func writeImageMetadata(image db.Image, manifest registry.ManifestWithBytesAndDigest, cvmfsRepo string) (db.TaskPtr, error) {
	titleStr := fmt.Sprintf("Write image metadata for %s to %s", image.GetSimpleName(), cvmfsRepo)
	task, ptr, err := db.CreateTask(nil, db.TASK_WRITE_IMAGE_METADATA, titleStr)
	if err != nil {
		return db.NullTaskPtr(), err
	}

	fetchConfigTask, err := registry.FetchAndParseConfigTask(image, manifest.Manifest.Config.Digest)
	if err != nil {
		task.LogFatal(nil, fmt.Sprintf("Failed to create fetch config task: %s", err.Error()))
		return ptr, nil
	}
	if err := task.LinkSubtask(nil, fetchConfigTask); err != nil {
		task.LogFatal(nil, fmt.Sprintf("Failed to link fetch config task: %s", err.Error()))
		return ptr, nil
	}

	go func() {
		task.Log(nil, db.LOG_SEVERITY_DEBUG, "Waiting for task to start")
		task.WaitForStart()
		task.Log(nil, db.LOG_SEVERITY_INFO, "Task started")

		// First, get the config
		task.Log(nil, db.LOG_SEVERITY_INFO, "Starting download of image config")
		fetchConfigTask.Start(nil)

		if !db.TaskResultSuccessful(fetchConfigTask.WaitUntilDone()) {
			task.LogFatal(nil, "Failed to fetch and parse config")
			return
		}
		artifact, err := fetchConfigTask.GetArtifact()
		if err != nil {
			task.LogFatal(nil, fmt.Sprintf("Failed to get artifact from fetch config task: %s", err.Error()))
			return
		}
		config, ok := artifact.(registry.ConfigWithBytesAndDigest)
		if !ok {
			task.LogFatal(nil, fmt.Sprintf("Invalid config type: %s", reflect.TypeOf(artifact).String()))
			return
		}
		task.Log(nil, db.LOG_SEVERITY_INFO, "Successfully downloaded image config")

		cvmfsLock.Lock()
		defer cvmfsLock.Unlock()

		abort := false
		task.Log(nil, db.LOG_SEVERITY_DEBUG, "Starting CVMFS transaction")
		if err := cvmfs.WithinTransaction(cvmfsRepo, func() error {
			imagePath := filepath.Join("/cvmfs", cvmfsRepo, constants.ImagesSubDir, manifest.ManifestDigest.Encoded()[:2], manifest.ManifestDigest.Encoded())
			// Ensure the image directory exists
			if err := os.MkdirAll(imagePath, constants.DirPermision); err != nil {
				task.LogFatal(nil, fmt.Sprintf("Failed to create image directory: %s", err.Error()))
				abort = true
				return err
			}
			// Write manifest
			if err := os.WriteFile(filepath.Join(imagePath, "manifest.json"), manifest.ManifestBytes, constants.FilePermision); err != nil {
				task.LogFatal(nil, fmt.Sprintf("Failed to write manifest: %s", err.Error()))
				abort = true
				return err
			}
			// Write config
			if err := os.WriteFile(filepath.Join(imagePath, "config.json"), config.ConfigBytes, constants.FilePermision); err != nil {
				task.LogFatal(nil, fmt.Sprintf("Failed to write config: %s", err.Error()))
				abort = true
				return err
			}
			return nil
		}); err != nil {
			task.LogFatal(nil, fmt.Sprintf("CvmFS transaction failed: %s", err.Error()))
			return
		}
		if abort {
			task.LogFatal(nil, "CvmFS transaction aborted")
			return
		}

		task.Log(nil, db.LOG_SEVERITY_DEBUG, "Successfully wrote image metadata")
		task.SetTaskCompleted(nil, db.TASK_RESULT_SUCCESS)
	}()

	return ptr, nil
}
