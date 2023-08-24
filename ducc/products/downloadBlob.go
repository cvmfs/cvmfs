package products

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
		useCount[blobDigest] = useCount[blobDigest] + 1
		downloadsMutex.Unlock()
		return existingTask, nil
	}

	titleStr := fmt.Sprintf("Download blob %s", blobDigest.String())
	task, ptr, err := db.CreateTask(nil, db.TASK_DOWNLOAD_BLOB, titleStr)
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
			// If the download failed, we remove the task from the pending downloads
			// and file use count maps.
			// The next time the blob is requested, it will therefore be downloaded again.
			downloadsMutex.Lock()
			delete(pendingDownloads, blobDigest)
			delete(useCount, blobDigest)
			downloadsMutex.Unlock()
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
		os.Remove(path.Join(config.TMP_FILE_PATH, "blobs", fileDigest.Encoded()))
		return
	}
	useCount[fileDigest] = count
}
