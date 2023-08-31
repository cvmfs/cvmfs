package products

import (
	"archive/tar"
	"compress/gzip"
	"errors"
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
	"github.com/cvmfs/ducc/singularity"
	"github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
)

type Chain = []ChainLink

type ChainLink struct {
	LayerDigest digest.Digest
	Compressed  bool

	ChainDigest         digest.Digest
	PreviousChainDigest digest.Digest
}

type ingestChainLinkKey struct {
	digest    digest.Digest
	cvmfsRepo string
}

var currentlyIngestingLinksMutex = sync.Mutex{}
var currentlyIngestingLinks = make(map[ingestChainLinkKey]db.TaskPtr)

func CreateFlat(image db.Image, manifest registry.ManifestWithBytesAndDigest, cvmfsRepo string) (db.TaskPtr, error) {
	titleStr := fmt.Sprintf("Create flat image for %s in %s", image.GetSimpleName(), cvmfsRepo)
	task, ptr, err := db.CreateTask(nil, db.TASK_CREATE_FLAT, titleStr)
	if err != nil {
		return db.NullTaskPtr(), err
	}

	// Check if the manifest contains foreign layers
	if ManifestContainsForeignLayers(manifest.Manifest) {
		task.LogFatal(nil, "Manifest contains foreign layers. Aborting")
		return ptr, nil
	}

	// Generate the chain
	chain := GenerateChainFromManifest(manifest.Manifest)

	// Ingest the chain
	chainTaskPtr, err := createChainForImage(image, chain, cvmfsRepo)
	if err != nil {
		task.LogFatal(nil, "Failed to create chain ingest task")
		return ptr, nil
	}
	err = task.LinkSubtask(nil, chainTaskPtr)
	if err != nil {
		task.LogFatal(nil, "Failed to add chain ingest task as subtask")
		return ptr, nil
	}

	// Create the required singularity files
	singularityFilesTaskPtr, err := createSingularityFiles(image, manifest, chain, cvmfsRepo)
	if err != nil {
		task.LogFatal(nil, "Failed to create singularity files task")
		return ptr, nil
	}
	task.LinkSubtask(nil, singularityFilesTaskPtr)
	if err != nil {
		task.LogFatal(nil, "Failed to add singularity files task as subtask")
		return ptr, nil
	}

	go func() {
		task.Log(nil, db.LOG_SEVERITY_DEBUG, "Waiting to start task")
		if !task.WaitForStart() {
			return
		}
		task.Log(nil, db.LOG_SEVERITY_INFO, "Started task")
		task.SetTaskStatus(nil, db.TASK_STATUS_RUNNING)
		chainTaskPtr.Start(nil)
		task.Log(nil, db.LOG_SEVERITY_INFO, "Started creating chain, waiting for it to finish")
		result := chainTaskPtr.WaitUntilDone()
		if result != db.TASK_RESULT_SUCCESS {
			task.LogFatal(nil, "Chain ingest failed")
			return
		}
		task.Log(nil, db.LOG_SEVERITY_INFO, "Successfully created chain")

		// Create singularity files
		singularityFilesTaskPtr.Start(nil)
		task.Log(nil, db.LOG_SEVERITY_INFO, "Started singularity files creation, waiting for it to finish")
		result = singularityFilesTaskPtr.WaitUntilDone()
		if result != db.TASK_RESULT_SUCCESS {
			task.LogFatal(nil, "Singularity files creation failed")
			return
		}
		task.Log(nil, db.LOG_SEVERITY_INFO, "Successfully created singularity files")

		task.SetTaskCompleted(nil, db.TASK_RESULT_SUCCESS)
		task.Log(nil, db.LOG_SEVERITY_INFO, "Task completed")
	}()

	return ptr, nil
}

func createChainForImage(image db.Image, chain Chain, cvmfsRepo string) (db.TaskPtr, error) {
	titleStr := fmt.Sprintf("Create chain for %s in %s", image.GetSimpleName(), cvmfsRepo)
	task, ptr, err := db.CreateTask(nil, db.TASK_CREATE_CHAIN, titleStr)
	if err != nil {
		return db.NullTaskPtr(), err
	}

	prevLinkTask := db.NullTaskPtr()
	for _, chainLink := range chain {
		subTask, err := createChainLink(chainLink, image, cvmfsRepo, prevLinkTask)
		if err != nil {
			task.LogFatal(nil, fmt.Sprintf("Failed to create \"%s\" task: %s", db.TASK_CREATE_CHAIN_LINK, err))
			return ptr, nil
		}
		err = task.LinkSubtask(nil, subTask)
		if err != nil {
			task.LogFatal(nil, fmt.Sprintf("Failed to set \"%s\" tasks as subtask: %s", db.TASK_CREATE_CHAIN_LINK, err))
			return ptr, nil
		}
		prevLinkTask = subTask
	}

	go func() {
		if !task.WaitForStart() {
			return
		}

		cvmfs.GetLock(cvmfsRepo)
		// Create the .chains directory with a catalog if it doesn't exist
		success, err := cvmfs.WithinTransactionNew(cvmfsRepo, func() error {
			_, err := cvmfs.CreateCatalogNew(filepath.Join("/cvmfs", "cvmfsRepo", ".chains"))
			if err != nil {
				task.Log(nil, db.LOG_SEVERITY_ERROR, fmt.Sprintf("Failed to create catalog: %s", err))
				return err
			}
			return nil
		})
		cvmfs.Unlock(cvmfsRepo)
		if err != nil || !success {
			task.LogFatal(nil, fmt.Sprintf("Failed to create .chains directory: %s", err))
			return
		}

		// Start the subtasks, building the chain links
		isError := false
		task.Log(nil, db.LOG_SEVERITY_INFO, "Started creating chain links.")
		for _, subTaskPtr := range task.Subtasks {
			if isError {
				subTaskPtr.Cancel(nil)
				continue
			}
			_, err := subTaskPtr.Start(nil)
			if err != nil {
				task.Log(nil, db.LOG_SEVERITY_ERROR, fmt.Sprintf("Failed to start subtask: %s", err))
				isError = true
			}
		}
		if isError {
			task.LogFatal(nil, "Could not create all chain links")
			return
		}

		// Wait for the subtasks to finish. Even though they depend on each other, by starting
		// them, we allow the downloads to happen in parallel.
		for _, subTaskPtr := range task.Subtasks {
			if isError {
				subTaskPtr.Cancel(nil)
				continue
			}
			if !db.TaskResultSuccessful(subTaskPtr.WaitUntilDone()) {
				task.Log(nil, db.LOG_SEVERITY_ERROR, "Failed while creating a chain link")
				isError = true
			}
		}
		if isError {
			task.LogFatal(nil, "Could not create all chain links")
			return
		}

		task.SetTaskCompleted(nil, db.TASK_RESULT_SUCCESS)
		task.Log(nil, db.LOG_SEVERITY_INFO, "Successfully created all chain links")
	}()

	return ptr, nil

}

func createSingularityFiles(image db.Image, manifest registry.ManifestWithBytesAndDigest, chain Chain, cvmfsRepo string) (db.TaskPtr, error) {
	titleStr := fmt.Sprintf("Create singularity files for %s in %s", image.GetSimpleName(), cvmfsRepo)
	task, ptr, err := db.CreateTask(nil, db.TASK_CREATE_SINGULARITY_FILES, titleStr)
	if err != nil {
		return db.NullTaskPtr(), err
	}

	fetchConfigTask, err := registry.FetchAndParseConfigTask(image, manifest.Manifest.Config.Digest)
	if err != nil {
		task.LogFatal(nil, fmt.Sprintf("Failed to create download config task: %s", err))
		return ptr, nil
	}

	if err := task.LinkSubtask(nil, fetchConfigTask); err != nil {
		task.LogFatal(nil, fmt.Sprintf("Failed to add download config task as subtask: %s", err))
		return ptr, nil
	}

	go func() {
		if !task.WaitForStart() {
			return
		}

		task.Log(nil, db.LOG_SEVERITY_DEBUG, "Waiting for CVMFS lock")
		cvmfs.GetLock(cvmfsRepo)
		once := sync.Once{}
		defer once.Do(func() {
			cvmfs.Unlock(cvmfsRepo)
			task.Log(nil, db.LOG_SEVERITY_DEBUG, "Released CVMFS lock")
		})
		task.Log(nil, db.LOG_SEVERITY_DEBUG, "Acquired CVMFS lock")

		var tagOrDigest string
		if image.Tag != "" {
			tagOrDigest = image.Tag
		} else {
			tagOrDigest = manifest.ManifestDigest.Encoded()
		}

		publicSymlinkPathShort := path.Join(image.RegistryHost, image.Repository+":"+tagOrDigest)
		publicSymlinkPath := path.Join("/cvmfs", cvmfsRepo, publicSymlinkPathShort)
		var publicSymlinkExists bool
		publicSymlinkInfo, err := os.Stat(publicSymlinkPath)
		if errors.Is(err, os.ErrNotExist) {
			publicSymlinkExists = false
		} else if err != nil {
			task.LogFatal(nil, fmt.Sprintf("Failed to stat public flat symlink: %s", err))
			return
		} else {
			publicSymlinkExists = true
		}

		privatePathShort := path.Join(".flat", manifest.ManifestDigest.Encoded()[:2], manifest.ManifestDigest.Encoded())
		privatePath := path.Join("/cvmfs", cvmfsRepo, privatePathShort)
		var privatePathExists bool
		privatePathInfo, err := os.Stat(privatePath)
		if errors.Is(err, os.ErrNotExist) {
			privatePathExists = false
		} else if err != nil {
			task.LogFatal(nil, fmt.Sprintf("Failed to stat private flat directory: %s", err))
			return
		} else {
			privatePathExists = true
		}

		if privatePathExists {
			if publicSymlinkExists && os.SameFile(publicSymlinkInfo, privatePathInfo) {
				task.Log(nil, db.LOG_SEVERITY_INFO, "Both public and private flat directories already up to date")
				fetchConfigTask.Skip(nil)
				task.SetTaskCompleted(nil, db.TASK_RESULT_SUCCESS)
				return
			}
			task.Log(nil, db.LOG_SEVERITY_DEBUG, "Public symlink not up to date, creating new")
			success, err := cvmfs.WithinTransactionNew(cvmfsRepo, func() error {
				if err := os.MkdirAll(path.Dir(publicSymlinkPath), constants.DirPermision); err != nil {
					task.LogFatal(nil, fmt.Sprintf("Failed to create public flat symlink directory: %s", err))
					return err
				}
				relativePath, err := filepath.Rel(path.Dir(publicSymlinkPath), privatePath)
				if err != nil {
					task.LogFatal(nil, fmt.Sprintf("Failed to create relative path for public flat symlink: %s", err))
					return err
				}
				if err := os.Symlink(relativePath, publicSymlinkPath); err != nil {
					task.LogFatal(nil, fmt.Sprintf("Failed to create public flat symlink: %s", err))
					return err
				}
				return nil
			})
			if (!success) || err != nil {
				task.LogFatal(nil, fmt.Sprintf("CVMFS transaction failed: %s", err))
				return
			}
			task.Log(nil, db.LOG_SEVERITY_INFO, fmt.Sprintf("Successfully updated symlink, %s -> %s", publicSymlinkPathShort, privatePathShort))
			fetchConfigTask.Skip(nil)
			task.SetTaskCompleted(nil, db.TASK_RESULT_SUCCESS)
			return
		}

		once.Do(func() {
			cvmfs.Unlock(cvmfsRepo)
			task.Log(nil, db.LOG_SEVERITY_DEBUG, "Releasing CVMFS lock while downloading image config")
		})

		// We need to create the private flat directory, including config and runscript
		// For that, we need to download the image config.
		task.Log(nil, db.LOG_SEVERITY_INFO, "Starting download of image config object")
		fetchConfigTask.Start(nil)
		if !db.TaskResultSuccessful(fetchConfigTask.WaitUntilDone()) {
			task.LogFatal(nil, "Failed to fetch and parse config object")
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

		task.Log(nil, db.LOG_SEVERITY_DEBUG, "Waiting for CVMFS lock")
		cvmfs.GetLock(cvmfsRepo)
		defer func() {
			cvmfs.Unlock(cvmfsRepo)
			task.Log(nil, db.LOG_SEVERITY_DEBUG, "Released CVMFS lock")
		}()
		lastChainDirectory := cvmfs.ChainPath(cvmfsRepo, chain[len(chain)-1].ChainDigest.Encoded())

		// First we create the private flat directory with catalog
		success, err := cvmfs.WithinTransactionNew(cvmfsRepo, func() error {
			if _, err := cvmfs.CreateCatalogNew(filepath.Join("/cvmfs", cvmfsRepo, ".flat")); err != nil {
				task.Log(nil, db.LOG_SEVERITY_ERROR, "Error in creating catalog inside `.flat` directory")
				return err
			}
			if err := os.MkdirAll(path.Dir(privatePath), constants.DirPermision); err != nil {
				task.Log(nil, db.LOG_SEVERITY_ERROR, "Error in creating directory inside .flat directory")
				return err
			}
			return nil
		})
		if !success || err != nil {
			task.LogFatal(nil, fmt.Sprintf("CVMFS transaction failed: %s", err))
			return
		}

		// Then we populate it using a template transaction
		success, err = cvmfs.WithinTransactionNew(cvmfsRepo, func() error {
			if err := singularity.MakeBaseEnv(privatePath); err != nil {
				task.Log(nil, db.LOG_SEVERITY_ERROR, "Error in creating the base singularity environment")
				return err
			}
			if err := singularity.InsertRunScript(privatePath, config.Config); err != nil {
				task.Log(nil, db.LOG_SEVERITY_ERROR, "Error in inserting the singularity runscript")
				return err
			}
			if err := singularity.InsertEnv(privatePath, config.Config); err != nil {
				task.Log(nil, db.LOG_SEVERITY_ERROR, "Error in inserting the singularity environment")
				return err
			}
			if err := os.MkdirAll(path.Dir(publicSymlinkPath), constants.DirPermision); err != nil {
				task.Log(nil, db.LOG_SEVERITY_ERROR, fmt.Sprintf("Failed to create image directory %s: %s", publicSymlinkPath, err))
				return err
			}
			// Finally, we create the public flat symlink
			relativePath, err := filepath.Rel(path.Dir(publicSymlinkPath), privatePath)
			if err != nil {
				task.Log(nil, db.LOG_SEVERITY_ERROR, fmt.Sprintf("Error creating symlink to .flat directory. Could not get relative path: %s", err))
				return err
			}
			if err := os.Symlink(relativePath, publicSymlinkPath); err != nil {
				task.Log(nil, db.LOG_SEVERITY_ERROR, fmt.Sprintf("Failed to create symlink to .flat direcotry: %s", err))
				return err
			}
			return nil
		},
			cvmfs.NewTemplateTransaction(cvmfs.TrimCVMFSRepoPrefix(lastChainDirectory), privatePathShort))
		if !success || err != nil {
			task.LogFatal(nil, fmt.Sprintf("CVMFS transaction failed: %s", err))
			return
		}

		task.SetTaskCompleted(nil, db.TASK_RESULT_SUCCESS)
		task.Log(nil, db.LOG_SEVERITY_INFO, "Task completed")
	}()

	return ptr, nil
}

func createChainLink(chainLink ChainLink, image db.Image, cvmfsRepo string, prevLinkTask db.TaskPtr) (db.TaskPtr, error) {
	// Check if the same chain link is already being ingested.
	// Then we can just wait for that task to finish.
	key := ingestChainLinkKey{chainLink.ChainDigest, cvmfsRepo}
	currentlyIngestingLinksMutex.Lock()
	if taskPtr, ok := currentlyIngestingLinks[key]; ok {
		currentlyIngestingLinksMutex.Unlock()
		return taskPtr, nil
	}
	titleStr := fmt.Sprintf("Create chain link %s for %s in %s", chainLink.ChainDigest.String(), image.GetSimpleName(), cvmfsRepo)
	task, ptr, err := db.CreateTask(nil, db.TASK_CREATE_CHAIN_LINK, titleStr)
	if err != nil {
		return db.TaskPtr{}, err
	}
	currentlyIngestingLinks[key] = ptr
	currentlyIngestingLinksMutex.Unlock()

	cvmfs.GetLock(cvmfsRepo)
	once := sync.Once{}
	defer once.Do(func() {
		cvmfs.Unlock(cvmfsRepo)
		task.Log(nil, db.LOG_SEVERITY_DEBUG, "Released CVMFS lock")
	})

	// If chain link already exists in CVMFS, we can skip it and return instantly
	exists, err := chainLinkExistsInCvmfs(chainLink, cvmfsRepo)
	if err != nil {
		task.LogFatal(nil, fmt.Sprintf("Failed to check if chain link exists in CVMFS: %s", err))
		return ptr, nil
	}
	if exists {
		task.Log(nil, db.LOG_SEVERITY_INFO, "Chain link already exists in CVMFS")
		task.SetTaskCompleted(nil, db.TASK_RESULT_SKIPPED)
		return ptr, nil
	}

	once.Do(func() {
		cvmfs.Unlock(cvmfsRepo)
		task.Log(nil, db.LOG_SEVERITY_DEBUG, "Releasing CVMFS lock")
	})

	// We create the download layer task
	registryPtr := registry.GetOrCreateRegistry(registry.ContainerRegistryIdentifier{Scheme: image.RegistryScheme, Hostname: image.RegistryHost})
	downloadLayerTaskPtr, err := DownloadBlob(registryPtr, image.Repository, chainLink.LayerDigest, nil)
	if err != nil {
		task.LogFatal(nil, fmt.Sprintf("Failed to create \"%s\" task: %s", db.TASK_DOWNLOAD_BLOB, err))
		return ptr, nil
	}
	// Want to release the blob as soon as we are done with it.
	// If we return early, we need to release it in this function.
	// If not, the task goroutine will release it.
	earlyReturn := true
	defer func() {
		if earlyReturn {
			releaseBlob(chainLink.LayerDigest)
		}
	}()
	if err := task.LinkSubtask(nil, downloadLayerTaskPtr); err != nil {
		task.LogFatal(nil, fmt.Sprintf("Failed to add \"%s\" task as subtask: %s", db.TASK_DOWNLOAD_BLOB, err))
		return ptr, nil
	}

	// We create the ingest layer task
	ingestChainLinkTaskPtr, err := ingestChainLink(chainLink, cvmfsRepo)
	if err != nil {
		task.LogFatal(nil, fmt.Sprintf("Failed to create \"%s\" task: %s", db.TASK_INGEST_CHAIN_LINK, err))
		return ptr, nil
	}
	if err := task.LinkSubtask(nil, ingestChainLinkTaskPtr); err != nil {
		task.LogFatal(nil, fmt.Sprintf("Failed to add \"%s\" task as subtask: %s", db.TASK_INGEST_CHAIN_LINK, err))
		return ptr, nil
	}

	earlyReturn = false
	go func() {
		defer releaseBlob(chainLink.LayerDigest)
		if !task.WaitForStart() {
			return
		}

		// We can begin the download layer task immediately
		downloadLayerTaskPtr.Start(nil)
		task.Log(nil, db.LOG_SEVERITY_INFO, "Started task for downloading layer, waiting for it to finish")
		result := downloadLayerTaskPtr.WaitUntilDone()
		if !db.TaskResultSuccessful(result) {
			task.LogFatal(nil, "Download layer failed")
			return
		}
		task.Log(nil, db.LOG_SEVERITY_INFO, "Successfully downloaded layer")

		// We then wait for the previous chain link to finish
		if prevLinkTask != db.NullTaskPtr() {
			task.Log(nil, db.LOG_SEVERITY_INFO, fmt.Sprintf("Waiting for ingestion of previous chain link (%s) to complete.", chainLink.PreviousChainDigest.String()))
			result := prevLinkTask.WaitUntilDone()
			if !db.TaskResultSuccessful(result) {
				task.LogFatal(nil, "Previous chain link failed")
				return
			}
			task.Log(nil, db.LOG_SEVERITY_INFO, "Previous chain link successfully ingested")
		}

		// And then we can actually ingest this chain link
		ingestChainLinkTaskPtr.Start(nil)
		task.Log(nil, db.LOG_SEVERITY_INFO, "Started task for ingesting chain link, waiting for it to finish")
		result = ingestChainLinkTaskPtr.WaitUntilDone()
		if !db.TaskResultSuccessful(result) {
			task.LogFatal(nil, "Ingest chain link failed")
			return
		}
		task.Log(nil, db.LOG_SEVERITY_INFO, "Successfully ingested chain link")

		// Remove the task from the currently ingesting map
		currentlyIngestingLinksMutex.Lock()
		delete(currentlyIngestingLinks, key)
		currentlyIngestingLinksMutex.Unlock()

		task.SetTaskCompleted(nil, db.TASK_RESULT_SUCCESS)
		task.Log(nil, db.LOG_SEVERITY_INFO, "Task completed")
	}()

	return ptr, nil
}

func ingestChainLink(link ChainLink, cvmfsRepo string) (db.TaskPtr, error) {
	titleStr := fmt.Sprintf("Ingest chain link %s into %s", link.ChainDigest.String(), cvmfsRepo)
	task, ptr, err := db.CreateTask(nil, db.TASK_INGEST_CHAIN_LINK, titleStr)
	if err != nil {
		return db.NullTaskPtr(), err
	}

	go func() {
		if !task.WaitForStart() {
			return
		}

		blobPath := path.Join(config.TMP_FILE_PATH, "blobs", link.LayerDigest.Encoded())
		fileReader, err := os.Open(blobPath)
		if err != nil {
			task.LogFatal(nil, fmt.Sprintf("Failed to open layer file %s: %s", blobPath, err))
			return
		}
		defer fileReader.Close()
		task.Log(nil, db.LOG_SEVERITY_DEBUG, fmt.Sprintf("Opened layer file %s", blobPath))
		var uncompressedReader io.ReadCloser
		if link.Compressed {
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
		tarReader := *tar.NewReader(readHashCloseSizer)

		previousChainID := ""
		if link.PreviousChainDigest != "" {
			// Want to avoid .Encoded() panicing if link.PreviousChainDigest is empty
			previousChainID = link.PreviousChainDigest.Encoded()
		}

		task.Log(nil, db.LOG_SEVERITY_INFO, "Waiting for CVMFS lock")
		cvmfs.GetLock(cvmfsRepo)
		defer cvmfs.Unlock(cvmfsRepo)
		task.Log(nil, db.LOG_SEVERITY_DEBUG, "Got CVMFS lock")

		err = cvmfs.CreateSneakyChain(cvmfsRepo, link.ChainDigest.Encoded(), previousChainID, tarReader)
		if err != nil {
			task.LogFatal(nil, fmt.Sprintf("Failed to ingest chain link: %s", err))
			return
		}

		task.SetTaskCompleted(nil, db.TASK_RESULT_SUCCESS)
		task.Log(nil, db.LOG_SEVERITY_INFO, "Task completed successfully")
	}()

	return ptr, nil
}

func chainLinkExistsInCvmfs(chainLink ChainLink, cvmfsRepo string) (bool, error) {
	_, err := os.Stat(cvmfs.ChainPath(cvmfsRepo, chainLink.ChainDigest.Encoded()))
	if err == nil {
		return true, nil
	} else if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func GenerateChainFromManifest(m v1.Manifest) Chain {
	// TODO: Make sure new chain id function is backwards compatible with old one
	chain := make([]ChainLink, len(m.Layers))

	for i, layer := range m.Layers {
		link := ChainLink{
			LayerDigest: layer.Digest,
			Compressed:  LayerMediaTypeIsCompressed(layer.MediaType),
		}

		if i == 0 {
			link.PreviousChainDigest = ""
			link.ChainDigest = link.LayerDigest
		} else {
			link.PreviousChainDigest = chain[i-1].ChainDigest
			link.ChainDigest = digest.FromString(fmt.Sprintf("%s %s", link.PreviousChainDigest.Encoded(), link.LayerDigest.Encoded()))
		}

		chain[i] = link
	}
	return chain
}
