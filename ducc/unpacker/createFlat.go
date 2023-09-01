package unpacker

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
	"github.com/cvmfs/ducc/cvmfs"
	"github.com/cvmfs/ducc/db"
	"github.com/cvmfs/ducc/registry"
	singularity "github.com/cvmfs/ducc/unpacker/singularity"
	"github.com/cvmfs/ducc/util"
	"github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	"golang.org/x/sys/unix"
)

type Chain = []ChainLink

type ChainLink struct {
	LayerDigest digest.Digest
	Compressed  bool

	LegacyChainDigest         digest.Digest
	PreviousLegacyChainDigest digest.Digest
}

type ingestChainLinkKey struct {
	legacyChainDigest digest.Digest
	cvmfsRepo         string
}

var currentlyIngestingLinksMutex = sync.Mutex{}
var currentlyIngestingLinks = make(map[ingestChainLinkKey]db.TaskPtr)

func ChainPath(cvmfsRepo string, legacyChainDigest digest.Digest) string {
	return filepath.Join("/", "cvmfs", cvmfsRepo, config.ChainSubDir, legacyChainDigest.Encoded()[0:2], legacyChainDigest.Encoded())
}

func DirtyChainPath(cvmfsRepo string, legacyChainDigest digest.Digest) string {
	return filepath.Join("/", "cvmfs", cvmfsRepo, config.DirtyChainSubDir, legacyChainDigest.Encoded())
}

func CreateFlat(image db.Image, manifest registry.ManifestWithBytesAndDigest, cvmfsRepo string) (db.TaskPtr, error) {
	titleStr := fmt.Sprintf("Create flat image for %s in %s", image.GetSimpleName(), cvmfsRepo)
	task, ptr, err := db.CreateTask(nil, db.TASK_CREATE_FLAT, titleStr)
	if err != nil {
		return db.NullTaskPtr(), err
	}

	// Check if the manifest contains foreign layers
	if registry.ManifestContainsForeignLayers(manifest.Manifest) {
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
		if !task.WaitForStart() {
			return
		}
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
			_, err := cvmfs.CreateCatalogNew(filepath.Join("/cvmfs", cvmfsRepo, ".chains"))
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
				if err := os.MkdirAll(path.Dir(publicSymlinkPath), config.DirPermision); err != nil {
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
		imageConfig, ok := artifact.(registry.ConfigWithBytesAndDigest)
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
		lastChainDirectory := ChainPath(cvmfsRepo, chain[len(chain)-1].LegacyChainDigest)

		// First we create the private flat directory with catalog
		success, err := cvmfs.WithinTransactionNew(cvmfsRepo, func() error {
			if _, err := cvmfs.CreateCatalogNew(filepath.Join("/cvmfs", cvmfsRepo, ".flat")); err != nil {
				task.Log(nil, db.LOG_SEVERITY_ERROR, "Error in creating catalog inside `.flat` directory")
				return err
			}
			if err := os.MkdirAll(path.Dir(privatePath), config.DirPermision); err != nil {
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
			if err := singularity.InsertRunScript(privatePath, imageConfig.Config); err != nil {
				task.Log(nil, db.LOG_SEVERITY_ERROR, "Error in inserting the singularity runscript")
				return err
			}
			if err := singularity.InsertEnv(privatePath, imageConfig.Config); err != nil {
				task.Log(nil, db.LOG_SEVERITY_ERROR, "Error in inserting the singularity environment")
				return err
			}
			if err := os.MkdirAll(path.Dir(publicSymlinkPath), config.DirPermision); err != nil {
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
	key := ingestChainLinkKey{chainLink.LegacyChainDigest, cvmfsRepo}
	currentlyIngestingLinksMutex.Lock()
	if taskPtr, ok := currentlyIngestingLinks[key]; ok {
		currentlyIngestingLinksMutex.Unlock()
		return taskPtr, nil
	}
	titleStr := fmt.Sprintf("Create chain link %s for %s in %s", chainLink.LegacyChainDigest.String(), image.GetSimpleName(), cvmfsRepo)
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
	// If it is dirty, we need to clean it up before re-ingesting it
	exists, dirty, err := checkChainLink(chainLink, cvmfsRepo)
	if err != nil {
		task.LogFatal(nil, fmt.Sprintf("Failed to check if chain link exists in CVMFS: %s", err))
		return ptr, nil
	}
	if dirty {
		task.Log(nil, db.LOG_SEVERITY_INFO, "The chain link is dirty. Likely somethink went wrong during ingestion. Cleaning up before continuing")
		if success, err := cvmfs.WithinTransactionNew(cvmfsRepo, func() error {
			if err := os.RemoveAll(ChainPath(cvmfsRepo, chainLink.LegacyChainDigest)); err != nil {
				task.Log(nil, db.LOG_SEVERITY_ERROR, fmt.Sprintf("Failed to remove dirty chain link: %s", err))
				return err
			}
			if err := os.RemoveAll(DirtyChainPath(cvmfsRepo, chainLink.LegacyChainDigest)); err != nil {
				task.Log(nil, db.LOG_SEVERITY_ERROR, fmt.Sprintf("Failed to remove dirty chain link flag: %s", err))
				return err
			}
			return nil
		}); !success || err != nil {
			task.LogFatal(nil, fmt.Sprintf("CVMFS transaction failed: %s", err))
			return ptr, nil
		}
	} else if exists {
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
	downloadLayerTaskPtr, err := registry.DownloadBlob(registryPtr, image.Repository, chainLink.LayerDigest, nil)
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
			registry.ReleaseBlob(chainLink.LayerDigest)
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
		defer registry.ReleaseBlob(chainLink.LayerDigest)
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
			task.Log(nil, db.LOG_SEVERITY_INFO, fmt.Sprintf("Waiting for ingestion of previous chain link (%s) to complete.", chainLink.PreviousLegacyChainDigest.String()))
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
	titleStr := fmt.Sprintf("Ingest chain link %s into %s", link.LegacyChainDigest.String(), cvmfsRepo)
	task, ptr, err := db.CreateTask(nil, db.TASK_INGEST_CHAIN_LINK, titleStr)
	if err != nil {
		return db.NullTaskPtr(), err
	}

	go func() {
		if !task.WaitForStart() {
			return
		}

		blobPath := filepath.Join(config.DownloadsDir, link.LayerDigest.Encoded())
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
		readHashCloseSizer := util.NewReadAndHash(uncompressedReader)
		tarReader := *tar.NewReader(readHashCloseSizer)

		task.Log(nil, db.LOG_SEVERITY_INFO, "Waiting for CVMFS lock")
		cvmfs.GetLock(cvmfsRepo)
		defer cvmfs.Unlock(cvmfsRepo)
		task.Log(nil, db.LOG_SEVERITY_DEBUG, "Got CVMFS lock")

		err = CreateSneakyChain(cvmfsRepo, link.LegacyChainDigest, link.PreviousLegacyChainDigest, tarReader)
		if err != nil {
			task.LogFatal(nil, fmt.Sprintf("Failed to ingest chain link: %s", err))
			return
		}

		task.SetTaskCompleted(nil, db.TASK_RESULT_SUCCESS)
		task.Log(nil, db.LOG_SEVERITY_INFO, "Task completed successfully")
	}()

	return ptr, nil
}

func checkChainLink(chainLink ChainLink, cvmfsRepo string) (exists bool, dirty bool, err error) {
	if _, err := os.Stat(ChainPath(cvmfsRepo, chainLink.LegacyChainDigest)); err != nil {
		// For some reason, we have to cast this to a PathError.
		// If not, errors.Is(os.ErrNotExist, err) will return false when a parent directory does not exist.
		var pathErr *os.PathError
		if errors.As(err, &pathErr) {
			err = pathErr.Err
		}
		if errors.Is(err, os.ErrNotExist) {
			return false, false, nil
		}
		return false, false, err
	}

	// Chain link exists, but we need to check if it is dirty
	// We do this by checking if the dirty chain directory exists
	if _, err := os.Stat(DirtyChainPath(cvmfsRepo, chainLink.LegacyChainDigest)); err != nil {
		var pathErr *os.PathError
		if errors.As(err, &pathErr) {
			err = pathErr.Err
		}
		if errors.Is(err, os.ErrNotExist) {
			return true, false, nil
		}
		return true, false, err

	}

	// Dirty chain directory exists
	return true, true, nil
}

func GenerateChainFromManifest(m v1.Manifest) Chain {
	// TODO: Make sure new chain id function is backwards compatible with old one
	chain := make([]ChainLink, len(m.Layers))

	for i, layer := range m.Layers {
		link := ChainLink{
			LayerDigest: layer.Digest,
			Compressed:  registry.LayerMediaTypeIsCompressed(layer.MediaType),
		}

		if i == 0 {
			link.PreviousLegacyChainDigest = ""
			link.LegacyChainDigest = link.LayerDigest
		} else {
			link.PreviousLegacyChainDigest = chain[i-1].LegacyChainDigest
			link.LegacyChainDigest = digest.FromString(fmt.Sprintf("%s %s", link.PreviousLegacyChainDigest.Encoded(), link.LayerDigest.Encoded()))
		}

		chain[i] = link
	}
	return chain
}

func CreateSneakyChain(cvmfsRepo string, newLegacyChainDigest digest.Digest, parentLegagyChainDigest digest.Digest, layer tar.Reader) error {
	sneakyPath := filepath.Join("/", "var", "spool", "cvmfs", cvmfsRepo, "scratch", "current")
	newChainPath := ChainPath(cvmfsRepo, newLegacyChainDigest)
	dirtyChainPath := DirtyChainPath(cvmfsRepo, newLegacyChainDigest)
	sneakyChainPath := filepath.Join(sneakyPath, cvmfs.TrimCVMFSRepoPrefix(newChainPath))

	// we need to create the directory were to do the template transaction
	dir := filepath.Dir(newChainPath)
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		// if the directory does not exists, we create it
		if success, err := cvmfs.WithinTransactionNew(cvmfsRepo, func() error {
			os.MkdirAll(dir, config.DirPermision)
			filePath := filepath.Join(dir, ".cvmfscatalog")
			f, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDONLY, config.FilePermision)
			if err != nil {
				return err
			}
			f.Close()
			// We mark the chain as dirty, in case something goes wrong
			err = os.MkdirAll(dirtyChainPath, config.DirPermision)
			if err != nil {
				return err
			}
			return nil
		}); err != nil {
			return err
		} else if !success {
			return fmt.Errorf("error in creating the directory for the new chain")
		}
	}
	// then we need the template transaction to populate it
	if parentLegagyChainDigest != "" {
		// if it is the very first chain, we don't need the template transaction
		opt := cvmfs.TemplateTransaction{
			Source:      cvmfs.TrimCVMFSRepoPrefix(ChainPath(cvmfsRepo, parentLegagyChainDigest)),
			Destination: cvmfs.TrimCVMFSRepoPrefix(newChainPath),
		}
		if success, err := cvmfs.WithinTransactionNew(cvmfsRepo, func() error {
			source := ChainPath(cvmfsRepo, parentLegagyChainDigest)
			sourceDirs, err := os.ReadDir(source)
			if err != nil {
				return err
			}
			destination := newChainPath
			destinationDirs, err := os.ReadDir(destination)
			if err != nil {
				return err
			}

			if len(sourceDirs) != len(destinationDirs) {
				return fmt.Errorf("Different number of directories between the source and tha target directories during a template transaction. source: %s , # of dir: %d, target: %s, # of dirs: %d", source, len(sourceDirs), destination, len(destinationDirs))
			}

			f, _ := os.OpenFile(filepath.Join(destination, ".cvmfscatalog"), os.O_CREATE|os.O_RDONLY, config.FilePermision)
			f.Close()
			return nil
		}, opt); err != nil {
			return err
		} else if !success {
			return fmt.Errorf("error in creating the template transaction for the new chain")
		}
	}

	// Finally we need the sneaky transaction to create the chain
	// We don't use a simple WithinTransaction, because we need to clean up the files in sneakyDir before abort
	closeSneakyTransactionOnce := sync.Once{}
	success, _, stdErr, err := cvmfs.OpenTransactionNew(cvmfsRepo)
	if err != nil {
		return fmt.Errorf("error in opening the sneaky transaction for the new chain: %s", err)
	} else if !success {
		return fmt.Errorf("error in opening the sneaky transaction for the new chain: %s", stdErr)
	}
	defer func() { closeSneakyTransactionOnce.Do(func() { cvmfs.AbortTransactionNew(cvmfsRepo) }) }()

	if func() error {
	loop:
		for {
			header, err := layer.Next()
			if err == io.EOF {
				f, err := os.OpenFile(filepath.Join(sneakyChainPath, ".cvmfscatalog"), os.O_CREATE|os.O_RDONLY, config.FilePermision)
				if err != nil {
					return fmt.Errorf("error in creating the .cvmfscatalog file: %w", err)
				}
				f.Close()
				return nil
			}

			if err != nil {
				return fmt.Errorf("error in reading the layer: %w", err)
			}

			if header == nil {
				continue loop
			}

			path := filepath.Join(sneakyChainPath, header.Name)
			dir := filepath.Dir(path)

			os.MkdirAll(dir, config.DirPermision)
			if cvmfs.IsWhiteout(path) {
				// this will be an empty file
				// check if it is an opaque directory or a standard whiteout file
				base := filepath.Base(path)
				if base == ".wh..wh..opq" {
					// an opaque directory
					if err := cvmfs.MakeOpaqueDir(dir); err != nil {
						return fmt.Errorf("error in making opaque directory: %w", err)
					}
				} else {
					// a whiteout file
					base = base[4:]
					path := filepath.Join(dir, base)
					if err := cvmfs.MakeWhiteoutFile(path); err != nil {
						return fmt.Errorf("error in making whiteout file: %w", err)
					}
				}
				continue
			}

			permissionMask := int64(0)
			switch header.Typeflag {

			case tar.TypeDir:
				{
					err := os.MkdirAll(path, config.DirPermision)
					if err != nil {
						return fmt.Errorf("error in creating directory: %w", err)
					}
					permissionMask |= 0700
				}
			case tar.TypeReg, tar.TypeRegA:
				{
					f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, config.FilePermision)
					if err != nil {
						return fmt.Errorf("error in creating file: %w", err)
					}
					if _, err = io.Copy(f, &layer); err != nil {
						f.Close()
						return fmt.Errorf("error in copying file from tar: %w", err)
					}
					f.Close()
					permissionMask |= 0600
				}
			case tar.TypeLink:
				{
					// hardlink
					// maybe we should just copy the file
					oldLinkName := filepath.Join(sneakyChainPath, header.Linkname)
					if err := os.Link(oldLinkName, path); err != nil {
						return fmt.Errorf("error in creating hard link: %w", err)
					}
				}
			case tar.TypeSymlink:
				{
					// symlink
					if err := os.Symlink(header.Linkname, path); err != nil {
						return fmt.Errorf("error in creating symbolic link: %w", err)
					}
					// TODO (smosciat)
					// do we want to invoke also Lchmod ?
					// the function does not really exist in std
					if err := os.Lchown(path, header.Uid, header.Gid); err != nil {
						return fmt.Errorf("error in chowning symbolic link: %w", err)
					}

					continue loop
				}
			case tar.TypeChar, tar.TypeBlock, tar.TypeFifo:
				{
					// char device
					var mode uint32
					switch header.Typeflag {
					case tar.TypeChar:
						mode = unix.S_IFCHR
					case tar.TypeBlock:
						mode = unix.S_IFBLK
					case tar.TypeFifo:
						mode = unix.S_IFIFO
					}
					dev := unix.Mkdev(uint32(header.Devmajor), uint32(header.Devminor))
					if err := unix.Mknod(path, uint32(os.FileMode(int64(mode)|header.Mode)), int(dev)); err != nil {
						return fmt.Errorf("error in creating special file: %w", err)
					}
				}
			default:
				{
					// unclear what to do here, just skip it
					continue loop
				}
			}

			// these are common to everything
			if err := os.Chmod(path, os.FileMode(header.Mode|permissionMask)); err != nil {
				return fmt.Errorf("error in chmod: %w", err)
			}
			if err := os.Chown(path, header.Uid, header.Gid); err != nil {
				return fmt.Errorf("error in chown: %w", err)
			}
			if err := os.Chtimes(path, header.AccessTime, header.ModTime); err != nil {
				return fmt.Errorf("error in chtimes: %w", err)
			}
		}
	}(); err != nil {
		// Clear our sneaky files
		os.RemoveAll(filepath.Join(sneakyPath, ".chains"))
		closeSneakyTransactionOnce.Do(func() { cvmfs.AbortTransactionNew(cvmfsRepo) })
		// Since something went wrong, better to just leave the .dirty flag
		// If the ingestion failed, likely an immediate cleanup attempt will fail as well
	}
	// everything went well, this flag is not necessary anymore
	os.RemoveAll(dirtyChainPath)
	// now the transaction is open and the sneaky overlay is populated
	// we don't need to do anything else at this point and we can close the transaction

	{
		// Publish the transaction.
		closeSneakyTransactionOnce.Do(func() {}) // Prevent the transaction from being aborted later
		success, _, stdErr, err := cvmfs.PublishTransactionNew(cvmfsRepo)
		if err != nil {
			return fmt.Errorf("error in publishing the sneaky transaction for the new chain: %w", err)
		}
		if !success {
			return fmt.Errorf("error in publishing the sneaky transaction for the new chain: %s", stdErr)
		}
	}
	return nil
}
