package unpacker

import (
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"sync"
	"time"

	"github.com/cvmfs/ducc/config"
	"github.com/cvmfs/ducc/cvmfs"
	"github.com/cvmfs/ducc/db"
	"github.com/cvmfs/ducc/registry"
	"github.com/cvmfs/ducc/util"
	"github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
)

type ingestLayerKey struct {
	CompressedLayerDigest digest.Digest
	cvmfsRepo             string
}

type Backlink struct {
	Origin []string `json:"origin"`
}

var currentlyIngestingLayersMutex = sync.Mutex{}
var currentlyIngestingLayers = make(map[ingestLayerKey]db.TaskPtr)

func LayerPath(cvmfsRepo string, compressedLayerDigest digest.Digest) string {
	return filepath.Join("/", "cvmfs", cvmfsRepo, config.SubDirInsideRepo, compressedLayerDigest.Encoded()[0:2], compressedLayerDigest.Encoded())
}

func LayerRootfsPath(cvmfsRepo string, compressedLayerDigest digest.Digest) string {
	return filepath.Join(LayerPath(cvmfsRepo, compressedLayerDigest), "layerfs")
}

func LayerMetadataPath(cvmfsRepo string, compressedLayerDigest digest.Digest) string {
	return filepath.Join(LayerPath(cvmfsRepo, compressedLayerDigest), ".metadata")
}

func BacklinkPath(cvmfsRepo string, compressedLayerDigest digest.Digest) string {
	return filepath.Join(LayerMetadataPath(cvmfsRepo, compressedLayerDigest), "origin.json")
}

func CreateLayers(image db.Image, manifest registry.ManifestWithBytesAndDigest, cvmfsRepo string) (db.TaskPtr, error) {
	titleStr := fmt.Sprintf("Ingest layers for %s to %s", image.GetSimpleName(), cvmfsRepo)
	task, ptr, err := db.CreateTask(nil, db.TASK_CREATE_LAYERS, titleStr)
	if err != nil {
		return db.NullTaskPtr(), err
	}

	createLayerTasks := make([]db.TaskPtr, len(manifest.Manifest.Layers))
	for i, layer := range manifest.Manifest.Layers {
		isCompressed := registry.LayerMediaTypeIsCompressed(layer.MediaType)
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

	createImageDataTask, err := createImageData(image, manifest, cvmfsRepo)
	if err != nil {
		task.LogFatal(nil, fmt.Sprintf("Failed to create task of type %s: %s", db.CREATE_IMAGE_DATA, err.Error()))
		return ptr, nil
	}
	if err := task.LinkSubtask(nil, createImageDataTask); err != nil {
		task.LogFatal(nil, fmt.Sprintf("Failed to add \"%s\" as subtask: %s", db.CREATE_IMAGE_DATA, err.Error()))
		return ptr, nil
	}

	go func() {
		if !task.WaitForStart() {
			return
		}

		// Check for the existence of the image in cvmfs
		// We do this while holding the lock to prevent concurrent ingestions
		cvmfs.GetLock(cvmfsRepo)
		once := sync.Once{}
		defer once.Do(func() { cvmfs.Unlock(cvmfsRepo) })
		alreadyConverted, err := imageAlreadyImported(cvmfsRepo, manifest.ManifestDigest, image.GetSimpleName())
		if err != nil {
			task.LogFatal(nil, fmt.Sprintf("Failed to check if image is already imported: %s", err.Error()))
			return
		}
		if alreadyConverted {
			task.Log(nil, db.LOG_SEVERITY_INFO, fmt.Sprintf("Image %s is already imported", image.GetSimpleName()))
			task.SetTaskCompleted(nil, db.TASK_RESULT_SKIPPED)
			return
		}
		task.Log(nil, db.LOG_SEVERITY_DEBUG, fmt.Sprintf("Image %s is not yet in CVMFS. Proceeding with ingestion", image.GetSimpleName()))
		once.Do(func() { cvmfs.Unlock(cvmfsRepo) })

		// Start creating the individual layers
		for _, createLayerTask := range createLayerTasks {
			createLayerTask.Start(nil)
		}
		task.Log(nil, db.LOG_SEVERITY_INFO, "Starting to create layers")
		failure := false
		for _, createLayerTask := range createLayerTasks {
			createLayerResult := createLayerTask.WaitUntilDone()
			if !db.TaskResultSuccessful(createLayerResult) {
				failure = true
			}
		}
		if failure {
			task.LogFatal(nil, "Not all layers were created successfully")
			return
		}
		task.Log(nil, db.LOG_SEVERITY_INFO, "Successfully created layers")

		// All layers creaded successfully. Now we can create the image data
		createImageDataTask.Start(nil)
		task.Log(nil, db.LOG_SEVERITY_INFO, "Layers created successfully. Starting to create image data")
		createImageDataResult := createImageDataTask.WaitUntilDone()
		if !db.TaskResultSuccessful(createImageDataResult) {
			task.LogFatal(nil, "Failed create image data.")
			return
		}
		task.Log(nil, db.LOG_SEVERITY_INFO, "Successfully created image data")

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
	cvmfs.GetLock(cvmfsRepo)
	once := sync.Once{}
	defer once.Do(func() { cvmfs.Unlock(cvmfsRepo) })
	exists, err := layerAlreadyOK(layerDigest, cvmfsRepo)
	if err != nil {
		task.LogFatal(nil, fmt.Sprintf("Failed to check if layer %s exists in cvmfs: %s", layerDigest.String(), err.Error()))
		return ptr, nil
	}
	if exists {
		task.Log(nil, db.LOG_SEVERITY_INFO, fmt.Sprintf("Layer %s already exists in cvmfs", layerDigest.String()))
		task.SetTaskCompleted(nil, db.TASK_RESULT_SKIPPED)
		return ptr, nil
	}
	task.Log(nil, db.LOG_SEVERITY_DEBUG, "Layer is not yet in cvmfs. Proceeding with ingestion")
	once.Do(func() { cvmfs.Unlock(cvmfsRepo) })

	// We create a task to download the layer
	registryPtr := registry.GetOrCreateRegistry(registry.ContainerRegistryIdentifier{Scheme: image.RegistryScheme, Hostname: image.RegistryHost})
	downloadLayerTask, err := registry.DownloadBlob(registryPtr, image.Repository, layerDigest, nil)
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
			registry.ReleaseBlob(layerDigest)
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
		defer registry.ReleaseBlob(layerDigest)
		if !task.WaitForStart() {
			return
		}
		// Start downloading the layer
		downloadLayerTask.Start(nil)
		task.Log(nil, db.LOG_SEVERITY_INFO, "Started task for downloading layer. Waiting for it to finish")
		downloadLayerResult := downloadLayerTask.WaitUntilDone()
		if !db.TaskResultSuccessful(downloadLayerResult) {
			task.LogFatal(nil, "Failed to download layer")
			ingestLayerTask.Cancel(nil)
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
		if !task.WaitForStart() {
			return
		}

		blobPath := path.Join(config.DownloadsDir, layerDigest.Encoded())
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
		readHashCloseSizer := util.NewReadAndHash(uncompressedReader)

		// BEGIN CVMFS METATRANSACTION
		// Ideally, this should be done in a transaction, as failing in the middle would
		// leave the repository in an inconsistent state. However, it is not possible to do
		// tar file ingestion in a transaction (yet). If this is ever implemented, this
		// could all be done in a single transaction.

		// However, we solve this by always re-ingesting if the metadata directory is not present
		// as this is the last step of the "meta-transaction"

		// We also put an internal lock on the cvmfs repo to prevent concurrent ingestions.
		task.Log(nil, db.LOG_SEVERITY_INFO, "Waiting for lock on cvmfs repo")
		cvmfs.GetLock(cvmfsRepo)
		defer cvmfs.Unlock(cvmfsRepo)
		task.Log(nil, db.LOG_SEVERITY_INFO, "Acquired lock on cvmfs repo")

		// First transaction: Create the catalog
		success, err := cvmfs.WithinTransactionNew(cvmfsRepo, func() error {
			// Check if the layer path already exists. If so, we assume that the ingestion failed in the middle
			// and we need to re-ingest the layer.
			_, err = os.Stat(LayerPath(cvmfsRepo, layerDigest))
			if !errors.Is(os.ErrNotExist, err) {
				task.Log(nil, db.LOG_SEVERITY_WARN, fmt.Sprintf("The layer seems to be partially ingested. Deleting it and re-ingesting"))
				if err := os.RemoveAll(LayerPath(cvmfsRepo, layerDigest)); err != nil {
					task.Log(nil, db.LOG_SEVERITY_ERROR, fmt.Sprintf("Failed to remove partial layer directory %s: %s", LayerPath(cvmfsRepo, layerDigest), err.Error()))
					return err
				}
			} else if err != nil {
				task.Log(nil, db.LOG_SEVERITY_ERROR, fmt.Sprintf("Failed to check if layer %s exists in cvmfs: %s", layerDigest.String(), err.Error()))
				return err
			}
			_, err = cvmfs.CreateCatalogNew(LayerPath(cvmfsRepo, layerDigest))
			if err != nil {
				task.Log(nil, db.LOG_SEVERITY_ERROR, fmt.Sprintf("Failed to create catalog for layer %s: %s", layerDigest.String(), err.Error()))
				return err
			}
			return nil
		})
		if (err != nil) || !success {
			task.LogFatal(nil, fmt.Sprintf("Failed to create catalog: %s", err.Error()))
			return
		}
		task.Log(nil, db.LOG_SEVERITY_DEBUG, fmt.Sprintf("Created catalog for layer %s", layerDigest.String()))

		// Second transaction: Ingest the layer FS
		task.Log(nil, db.LOG_SEVERITY_DEBUG, "Starting CVMFS for ingesting layer")
		ingestPath := cvmfs.TrimCVMFSRepoPrefix(LayerRootfsPath(cvmfsRepo, layerDigest))
		if err := cvmfs.Ingest(cvmfsRepo, readHashCloseSizer, "--catalog", "-t", "-", "-b", ingestPath); err != nil {
			task.LogFatal(nil, fmt.Sprintf("Failed to ingest layer %s: %s", layerDigest.String(), err.Error()))
			return
		}
		task.Log(nil, db.LOG_SEVERITY_DEBUG, "Successfully ingested layer file system")

		// Final transaction: Write the layer metadata
		success, err = cvmfs.WithinTransactionNew(cvmfsRepo, func() error {
			if err := storeLayerInfo(cvmfsRepo, layerDigest, readHashCloseSizer); err != nil {
				task.Log(nil, db.LOG_SEVERITY_ERROR, fmt.Sprintf("Failed to store layer metadata for layer %s: %s", layerDigest.String(), err.Error()))
				return err
			}
			return nil
		})
		if (err != nil) || !success {
			task.LogFatal(nil, fmt.Sprintf("Failed to store layer metadata for layer %s", layerDigest.String()))
			return
		}
		task.Log(nil, db.LOG_SEVERITY_DEBUG, fmt.Sprintf("Successfully stored layer metadata for layer %s", layerDigest.String()))
		// END CVMFS METATRANSACTION

		task.SetTaskCompleted(nil, db.TASK_RESULT_SUCCESS)
		task.Log(nil, db.LOG_SEVERITY_INFO, "Task completed successfully")
	}()

	return ptr, nil
}

func layerAlreadyOK(layerDigest digest.Digest, cvmfsRepo string) (bool, error) {
	// Check if the layer is already in cvmfs
	if _, err := os.Stat(LayerPath(cvmfsRepo, layerDigest)); err != nil {
		// For some reason, we have to cast this to a PathError.
		// If not, errors.Is(os.ErrNotExist, err) will return false when a parent directory does not exist.
		var pathErr *os.PathError
		if errors.As(err, &pathErr) {
			err = pathErr.Err
		}
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
	}

	// Check if the layer metadata is in cvmfs.
	// If not, the ingestion could have failed in the middle.
	path := LayerMetadataPath(cvmfsRepo, layerDigest)
	if _, err := os.Stat(path); err != nil {
		// For some reason, we have to cast this to a PathError.
		// If not, errors.Is(os.ErrNotExist, err) will return false when a parent directory does not exist.
		var pathErr *os.PathError
		if errors.As(err, &pathErr) {
			err = pathErr.Err
		}
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, err
	}

	return true, nil
}

func createImageData(image db.Image, manifest registry.ManifestWithBytesAndDigest, cvmfsRepo string) (db.TaskPtr, error) {
	titleStr := fmt.Sprintf("Create image data for %s in %s", image.GetSimpleName(), cvmfsRepo)
	task, ptr, err := db.CreateTask(nil, db.CREATE_IMAGE_DATA, titleStr)
	if err != nil {
		return db.NullTaskPtr(), err
	}

	go func() {
		if !task.WaitForStart() {
			return
		}
		cvmfs.GetLock(cvmfsRepo)
		defer cvmfs.Unlock(cvmfsRepo)
		success, err := cvmfs.WithinTransactionNew(cvmfsRepo, func() error {
			// Create the backlinks
			if err := createLayersBacklink(cvmfsRepo, manifest.Manifest, image.GetSimpleName()); err != nil {
				task.Log(nil, db.LOG_SEVERITY_ERROR, fmt.Sprintf("Failed to create backlinks: %s", err.Error()))
				return err
			}
			task.Log(nil, db.LOG_SEVERITY_DEBUG, "Created layer backlinks")

			// Create the image metadata dir, and add manifest.json
			manifestPath := filepath.Join("/cvmfs", cvmfsRepo, ".metadata", image.GetSimpleName(), "manifest.json")
			if err := os.MkdirAll(filepath.Dir(manifestPath), config.DirPermision); err != nil {
				task.Log(nil, db.LOG_SEVERITY_ERROR, fmt.Sprintf("Failed to create image metadata directory: %s", err.Error()))
				return err
			}
			err := os.WriteFile(manifestPath, manifest.ManifestBytes, config.FilePermision)
			if err != nil {
				task.Log(nil, db.LOG_SEVERITY_ERROR, fmt.Sprintf("Failed to write manifest.json: %s", err.Error()))
				return err
			}
			task.Log(nil, db.LOG_SEVERITY_DEBUG, "Created image metadata directory and manifest.json")
			return nil
		})
		if (err != nil) || !success {
			task.LogFatal(nil, "Failed to create image metadata")
			return
		}

		// TODO: Garbage collection

		task.SetTaskCompleted(nil, db.TASK_RESULT_SUCCESS)
		task.Log(nil, db.LOG_SEVERITY_INFO, "Task completed")

	}()

	return ptr, nil
}

func storeLayerInfo(CVMFSRepo string, compressedLayerDigest digest.Digest, r util.ReadHashCloseSizer) (err error) {
	layersdata := []LayerInfo{}
	layerInfoPath := filepath.Join(LayerMetadataPath(CVMFSRepo, compressedLayerDigest), "layers.json")

	diffID := fmt.Sprintf("%x", r.Sum(nil))
	size := r.GetSize()
	created := time.Now()
	layerinfo := LayerInfo{
		ID:                   diffID,
		Created:              created,
		CompressedDiffDigest: compressedLayerDigest.String(),
		UncompressedDigest:   "sha256:" + diffID,
		UncompressedSize:     size,
	}
	layersdata = append(layersdata, layerinfo)

	jsonLayerInfo, err := json.MarshalIndent(layersdata, "", " ")
	if err != nil {
		return err
	}

	// Create the directory if it doesn't exist
	if err := os.Mkdir(filepath.Dir(layerInfoPath), config.DirPermision); err != nil {
		return err
	}
	file, err := os.Create(layerInfoPath)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.Write(jsonLayerInfo)
	if err != nil {
		return err
	}
	return
}

func imageAlreadyImported(cvmfsRepo string, newManifestDigest digest.Digest, imageName string) (bool, error) {
	path := filepath.Join("/cvmfs", cvmfsRepo, ".metadata", imageName, "manifest.json")

	manifestStat, err := os.Stat(path)
	if os.IsNotExist(err) {
		return false, nil
	}
	if !manifestStat.Mode().IsRegular() {
		return false, fmt.Errorf("manifest is not a regular file")
	}

	file, err := os.Open(path)
	if err != nil {
		printErrorChain(err)
		return false, fmt.Errorf("failed to open manifest.json: %w", err)
	}
	defer file.Close()
	digest, err := digest.SHA256.FromReader(file)
	if err != nil {
		return false, fmt.Errorf("failed to calculate digest of manifest.json: %w", err)
	}
	return digest == newManifestDigest, nil
}

func printErrorChain(err error) {
	fmt.Println("Error chain:")
	for err != nil {
		fmt.Printf("- %T: %v\n", err, err)
		err = errors.Unwrap(err)
	}
}

func getBacklinkFromLayer(cvmfsRepo string, compressedLayerDigest digest.Digest) (backlink Backlink, err error) {
	backlinkPath := BacklinkPath(cvmfsRepo, compressedLayerDigest)

	if _, err := os.Stat(backlinkPath); os.IsNotExist(err) {
		return Backlink{Origin: []string{}}, nil
	}

	backlinkFile, err := os.Open(backlinkPath)
	if err != nil {
		return backlink, fmt.Errorf("error in opening the backlink file: %v", err)
	}

	byteBackLink, err := ioutil.ReadAll(backlinkFile)
	if err != nil {
		return backlink, fmt.Errorf("error in reading the bytes from the origin file: %v", err)
	}

	err = backlinkFile.Close()
	if err != nil {
		return backlink, fmt.Errorf("error in closing the file after reading: %v", err)
	}

	err = json.Unmarshal(byteBackLink, &backlink)
	if err != nil {
		return backlink, fmt.Errorf("error in unmarshaling the files: %v", err)
	}
	return backlink, nil
}

// Need to be in a transaction when calling this function
func createLayersBacklink(CVMFSRepo string, manifest v1.Manifest, imageName string) error {

	backlinks := make(map[string][]byte)

	for _, layerDescriptor := range manifest.Layers {
		configDigest := manifest.Config.Digest.String()

		backlink, err := getBacklinkFromLayer(CVMFSRepo, layerDescriptor.Digest)
		if err != nil {
			//TODO: Log "Error in obtaining the backlink from a compressed layer digest, skipping..."
		}
		backlink.Origin = append(backlink.Origin, configDigest)

		backlinkBytesMarshal, err := json.Marshal(backlink)
		if err != nil {
			// TODO: Log "error in marshaling backlinks"
			continue
		}

		backlinkPath := BacklinkPath(CVMFSRepo, layerDescriptor.Digest)
		backlinks[backlinkPath] = backlinkBytesMarshal
	}

	for path, fileContent := range backlinks {
		// the path may not be there, check,
		// and if it doesn't exists create it
		dir := filepath.Dir(path)
		if _, err := os.Stat(dir); os.IsNotExist(err) {
			err = os.MkdirAll(dir, config.DirPermision)
			if err != nil {
				// TODO: Log "Error in creating the directory for the backlinks file, skipping..."
				continue
			}
		}
		err := os.WriteFile(path, fileContent, config.FilePermision)
		if err != nil {
			// TODO: Log "Error in writing the backlinks file"
			continue
		}
	}
	return nil
}

func GetBacklinkFromLayer(cvmfsRepo string, compressedLayerDigest digest.Digest) (Backlink, error) {
	backlinkPath := BacklinkPath(cvmfsRepo, compressedLayerDigest)

	if _, err := os.Stat(backlinkPath); os.IsNotExist(err) {
		return Backlink{Origin: []string{}}, nil
	}

	backlinkFile, err := os.Open(backlinkPath)
	if err != nil {
		return Backlink{}, fmt.Errorf("error in opening the file for writing the backlinks: %w", err)
	}

	byteBackLink, err := io.ReadAll(backlinkFile)
	if err != nil {
		return Backlink{}, fmt.Errorf("error in reading the bytes from the origin file: %w", err)
	}

	err = backlinkFile.Close()
	if err != nil {
		return Backlink{}, fmt.Errorf("error in closing the file after reading: %w", err)
	}

	var backlink Backlink
	err = json.Unmarshal(byteBackLink, &backlink)
	if err != nil {
		return Backlink{}, fmt.Errorf("error in unmarshaling the files: %w", err)
	}

	return backlink, nil
}
