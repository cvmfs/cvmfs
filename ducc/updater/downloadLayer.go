package updater

import (
	"bufio"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"path"
	"sync"

	"github.com/cvmfs/ducc/config"
	"github.com/cvmfs/ducc/db"
	"github.com/cvmfs/ducc/registry"
	"github.com/opencontainers/go-digest"
)

var downloadsMutex = sync.Mutex{}
var pendingDownloads = make(map[digest.Digest]db.TaskPtr)
var useCount = make(map[digest.Digest]int)

func DownloadLayer(layerDigest digest.Digest) (db.TaskPtr, error) {
	// Check if the layer is already being downloaded
	// Then we can just wait for that task to finish.
	downloadsMutex.Lock()
	if existingTask, ok := pendingDownloads[layerDigest]; ok {
		useCount[layerDigest]++
		downloadsMutex.Unlock()
		fmt.Printf("Using existing download task for layer %s\n", layerDigest.String())
		return existingTask, nil
	}
	downloadsMutex.Unlock()

	task, ptr, err := db.CreateTask(nil, db.TASK_DOWNLOAD_BLOB)
	if err != nil {
		fmt.Printf("Failed to create download task for %s: %s\n", layerDigest.String(), err.Error())
		return db.TaskPtr{}, err
	}

	// Find from where we can download the layer
	images, err := db.GetImagesByLayerDigest(nil, layerDigest)
	if err != nil {
		task.LogFatal(nil, fmt.Sprintf("DB lookup failed: %s", err.Error()))
		fmt.Printf("DB lookup failed: %s\n", err.Error())
		return ptr, nil
	}
	if len(images) == 0 {
		task.LogFatal(nil, fmt.Sprintf("Could not find a registry that has image %s", layerDigest.String()))
		fmt.Printf("Could not find a registry that has image %s\n", layerDigest.String())
		return ptr, nil
	}

	go func() {
		// Download the layer
		task.WaitForStart()
		task.Log(nil, db.LOG_SEVERITY_DEBUG, fmt.Sprintf("Downloading layer %s", layerDigest.String()))
		// TODO: If a layer is available in multiple registries, we could load balance properly
		err := performLayerDownload(images[rand.Intn(len(images))], layerDigest)
		if err != nil {
			task.LogFatal(nil, fmt.Sprintf("Failed to download layer %s: %s", layerDigest.String(), err.Error()))
			return
		}
		task.Log(nil, db.LOG_SEVERITY_DEBUG, fmt.Sprintf("Finished downloading layer %s", layerDigest.String()))
		_ = task.SetTaskCompleted(nil, db.TASK_RESULT_SUCCESS)
	}()

	return ptr, nil
}

func performLayerDownload(image db.Image, layerDigest digest.Digest) error {
	url := fmt.Sprintf("%s://%s/v2/%s/blobs/%s", image.RegistryScheme, image.RegistryHost, image.Repository, layerDigest.String())

	// We need to handle the case where a file with the same name already exists
	// might as well check if it can be used directly to skip the download.

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return fmt.Errorf("error in creating request: %s", err)
	}
	registry := registry.GetOrCreateRegistry(registry.ContainerRegistryIdentifier{Scheme: image.RegistryScheme, Hostname: image.RegistryHost})
	res, err := registry.PerformRequest(req)
	if err != nil {
		return fmt.Errorf("error in fetching layer: %s", err)
	}
	defer res.Body.Close()

	if 200 > res.StatusCode || res.StatusCode >= 300 {
		return fmt.Errorf("error in fetching layer: %s", res.Status)
	}

	os.MkdirAll(path.Join(config.TMP_FILE_PATH, "blobs"), os.FileMode(0755))
	filePath := path.Join(config.TMP_FILE_PATH, "blobs", layerDigest.Encoded())
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.FileMode(0644))
	if err != nil {
		return fmt.Errorf("error in creating file: %s", err)
	}
	// Verify the checksum while writing to the file
	reader := bufio.NewReader(io.TeeReader(res.Body, file))
	var checksum digest.Digest
	var checksumErr error
	checksum, err = digest.SHA256.FromReader(reader)
	file.Close()
	if err != nil {
		err = fmt.Errorf("error in calculating checksum: %s", checksumErr)
	}
	if checksum != layerDigest {
		err = fmt.Errorf("checksum mismatch: expected %s, got %s", layerDigest.String(), checksum.String())
	}
	if err != nil {
		// Something went wrong, remove the file
		fmt.Printf("Error: %s\n", err.Error())
		os.Remove(filePath)
		return err
	}
	return nil
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
