package updater

import (
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path"
	"sync"

	"github.com/cvmfs/ducc/config"
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

func CreateLayers(image db.Image, manifest v1.Manifest, cvmfsRepo string) (db.TaskPtr, error) {
	task, ptr, err := db.CreateTask(nil, db.TASK_CREATE_LAYERS)
	if err != nil {
		return db.NullTaskPtr(), err
	}

	// Ingest the layers
	ingestLayersTask, err := ingestLayers(image, manifest, cvmfsRepo)
	if err != nil {
		task.LogFatal(nil, fmt.Sprintf("Failed to create task of type %s: %s", db.TASK_INGEST_LAYERS, err.Error()))
		return ptr, nil
	}
	if err := task.LinkSubtask(nil, ingestLayersTask); err != nil {
		task.LogFatal(nil, fmt.Sprintf("Failed to add \"%s\" as subtask: %s", db.TASK_INGEST_LAYERS, err.Error()))
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
			return
		}
		task.Log(nil, db.LOG_SEVERITY_INFO, "Successfully ingested layers")

		// Mark the task as completed
		task.SetTaskCompleted(nil, db.TASK_RESULT_SUCCESS)
		task.Log(nil, db.LOG_SEVERITY_INFO, "Task completed")
	}()

	return ptr, nil
}

func ingestLayers(image db.Image, manifest v1.Manifest, cvmfsRepo string) (db.TaskPtr, error) {
	task, ptr, err := db.CreateTask(nil, db.TASK_INGEST_LAYERS)
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
	task, ptr, err := db.CreateTask(nil, db.TASK_CREATE_LAYER)
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

	go func() {
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
	task, ptr, err := db.CreateTask(nil, db.TASK_INGEST_LAYER)
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

		layerPath := cvmfs.LayerPath(cvmfsRepo, layerDigest.Encoded())
		if err := cvmfs.CreateCatalogIntoDir(cvmfsRepo, layerPath); err != nil {
			cvmfs.RemoveDirectory(cvmfsRepo, layerPath)
			task.LogFatal(nil, fmt.Sprintf("Failed to create catalog for layer %s: %s", layerDigest.String(), err.Error()))
			return
		}
		task.Log(nil, db.LOG_SEVERITY_DEBUG, fmt.Sprintf("Created catalog for layer %s", layerDigest.String()))

		// Ingest the layer FS
		ingestPath := cvmfs.LayerRootfsPath(cvmfsRepo, layerDigest.Encoded())
		if err := cvmfs.Ingest(cvmfsRepo, readHashCloseSizer, "--catalog", "-t", "-", "-b", ingestPath); err != nil {
			cvmfs.RemoveDirectory(cvmfsRepo, layerPath)
			task.LogFatal(nil, fmt.Sprintf("Failed to ingest layer %s: %s", layerDigest.String(), err.Error()))
			return
		}
		task.Log(nil, db.LOG_SEVERITY_DEBUG, fmt.Sprintf("Successfully ingested layer %s", layerDigest.String()))

		// Write the layer metadata
		if err := lib.StoreLayerInfo(cvmfsRepo, layerDigest.Encoded(), readHashCloseSizer); err != nil {
			cvmfs.RemoveDirectory(cvmfsRepo, layerPath)
			task.LogFatal(nil, fmt.Sprintf("Failed to store layer metadata for layer %s: %s", layerDigest.String(), err.Error()))
			return
		}
		task.Log(nil, db.LOG_SEVERITY_DEBUG, fmt.Sprintf("Successfully stored layer metadata for layer %s", layerDigest.String()))
		// END CVMFS METATRANSACTION

		// Release the downloaded blob
		releaseBlob(layerDigest)
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
