package updater

import (
	"fmt"
	"os"
	"path"

	"github.com/cvmfs/ducc/config"
	"github.com/cvmfs/ducc/db"
	"github.com/cvmfs/ducc/registry"
	"github.com/opencontainers/go-digest"
)

func DownloadBlob(registry *registry.ContainerRegistry, repository string, blobDigest digest.Digest, acceptHeaders []string) (db.TaskPtr, error) {
	// Check if the blob is already being downloaded
	// Then we can just wait for that task to finish.
	downloadsMutex.Lock()
	if existingTask, ok := pendingDownloads[blobDigest]; ok {
		useCount[blobDigest]++
		downloadsMutex.Unlock()
		return existingTask, nil
	}

	task, ptr, err := db.CreateTask(nil, db.TASK_DOWNLOAD_BLOB)
	if err != nil {
		fmt.Printf("Failed to create download task for %s: %s\n", blobDigest.String(), err.Error())
		downloadsMutex.Unlock()
		return db.TaskPtr{}, err
	}
	// Register that we are downloading this layer
	pendingDownloads[blobDigest] = ptr
	useCount[blobDigest] = 1
	downloadsMutex.Unlock()

	go func() {
		// Download the layer
		task.WaitForStart()
		task.Log(nil, db.LOG_SEVERITY_DEBUG, fmt.Sprintf("Downloading blob %s", blobDigest.String()))
		err := registry.DownloadBlob(blobDigest, repository, acceptHeaders)
		if err != nil {
			task.LogFatal(nil, fmt.Sprintf("Failed to download blob %s: %s", blobDigest.String(), err.Error()))
			return
		}
		task.Log(nil, db.LOG_SEVERITY_DEBUG, fmt.Sprintf("Finished downloading blob %s", blobDigest.String()))
		_ = task.SetTaskCompleted(nil, db.TASK_RESULT_SUCCESS)
	}()

	return ptr, nil
}

func releaseBlob(fileDigest digest.Digest) {
	downloadsMutex.Lock()
	defer downloadsMutex.Unlock()
	count, ok := useCount[fileDigest]
	if !ok {
		return
	}
	count -= 1
	if count == 0 {
		delete(pendingDownloads, fileDigest)
		delete(useCount, fileDigest)
		os.Remove(path.Join(config.TMP_FILE_PATH, "blobs", fileDigest.String()))
		return
	}
	useCount[fileDigest] = count
}
