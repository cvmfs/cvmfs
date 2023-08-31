package products

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"sort"
	"sync"
	"time"

	"github.com/cvmfs/ducc/config"
	"github.com/cvmfs/ducc/cvmfs"
	"github.com/cvmfs/ducc/db"
	"github.com/cvmfs/ducc/registry"
	"github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
)

// struct for entries in images.json
type PodmanImageInfo struct {
	ID      string    `json:"id,omitempty"`
	Names   []string  `json:"names,omitempty"`
	Layer   string    `json:"layer,omitempty"`
	Created time.Time `json:"created,omitempty"`
}

// struct for entries in layers.json
type PodmanLayerInfo struct {
	ID                   string        `json:"id,omitempty"`
	ShortID              string        `json:"-"`
	Parent               string        `json:"parent,omitempty"`
	Created              time.Time     `json:"created,omitempty"`
	CompressedDiffDigest digest.Digest `json:"compressed-diff-digest,omitempty"`
	CompressedSize       int64         `json:"compressed-size,omitempty"`
	UncompressedDigest   digest.Digest `json:"diff-digest,omitempty"`
}

func CreatePodman(image db.Image, manifest registry.ManifestWithBytesAndDigest, cvmfsRepo string) (db.TaskPtr, error) {
	// TODO: Check if the image is already being created
	titleStr := fmt.Sprintf("Create Podman image for %s in %s", image.GetSimpleName(), cvmfsRepo)
	task, ptr, err := db.CreateTask(nil, db.TASK_CREATE_PODMAN, titleStr)
	if err != nil {
		return db.TaskPtr{}, err
	}

	// We need the config
	fetchConfigTask, err := registry.FetchAndParseConfigTask(image, manifest.Manifest.Config.Digest)
	if err != nil {
		task.LogFatal(nil, fmt.Sprintf("Failed to create fetch config task: %s", err.Error()))
		return ptr, err
	}
	if err := task.LinkSubtask(nil, fetchConfigTask); err != nil {
		task.LogFatal(nil, fmt.Sprintf("Failed to link fetch config task: %s", err.Error()))
		return ptr, err
	}

	go func() {
		if !task.WaitForStart() {
			return
		}
		task.Log(nil, db.LOG_SEVERITY_DEBUG, "Starting podman image creation")

		// 0. We need the config
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
		imageConfig, ok := artifact.(registry.ConfigWithBytesAndDigest)
		if !ok {
			task.LogFatal(nil, fmt.Sprintf("Invalid config type: %s", reflect.TypeOf(artifact).String()))
			return
		}

		// We need to lock CVMFS, and start a transaction
		task.Log(nil, db.LOG_SEVERITY_INFO, "Waiting for CVMFS lock")
		cvmfs.GetLock(cvmfsRepo)
		defer cvmfs.Unlock(cvmfsRepo)
		task.Log(nil, db.LOG_SEVERITY_INFO, "Got CVMFS lock, waiting for transaction")
		endTransactionOnce := sync.Once{}
		ok, stdOut, stdErr, err := cvmfs.OpenTransactionNew(cvmfsRepo)
		if err != nil {
			task.LogFatal(nil, fmt.Sprintf("Failed to open transaction: %s", err.Error()))
			return
		} else if !ok {
			task.LogFatal(nil, fmt.Sprintf("Failed to open transaction\nstdout:\n\n%s\nstderr:%s\n", stdOut, stdErr))
			return
		} else {
			task.Log(nil, db.LOG_SEVERITY_DEBUG, fmt.Sprintf("Successfully opened transaction\n%s", stdOut))
			// Abort the transaction if we exit early
			defer endTransactionOnce.Do(func() {
				err := cvmfs.AbortTransactionNew(cvmfsRepo)
				if err != nil {
					task.Log(nil, db.LOG_SEVERITY_ERROR, fmt.Sprintf("Failed to abort transaction: %s", err.Error()))
				}
				task.Log(nil, db.LOG_SEVERITY_DEBUG, "Aborted transaction")
			})
		}

		madeChanges := false // If we made changes, we need to commit them

		// 1. Ensure that podman directories and catalogs exist
		requiredCatalogs := []string{
			path.Join("/cvmfs/", config.PodmanSubDir),
			path.Join("/cvmfs/", cvmfsRepo, config.PodmanSubDir, "overlay"),
			path.Join("/cvmfs/", cvmfsRepo, config.PodmanSubDir, "overlay-images"),
			path.Join("/cvmfs/", cvmfsRepo, config.PodmanSubDir, "overlay-layers"),
		}
		for _, dir := range requiredCatalogs {
			changed, err := cvmfs.CreateCatalogNew(dir)
			if err != nil {
				task.LogFatal(nil, fmt.Sprintf("Failed to create catalog %s: %s", dir, err))
				return
			}
			if changed {
				task.Log(nil, db.LOG_SEVERITY_DEBUG, fmt.Sprintf("Created catalog %s", dir))
				madeChanges = true
			}
		}

		// 2. Create Podman layer metadata for the layers in the manifest
		manifestLayersInfo, err := createPodmanLayerInfo(manifest.Manifest, imageConfig.Config)
		if err != nil {
			task.LogFatal(nil, fmt.Sprintf("Failed to create layer metadata: %s", err.Error()))
			return
		}
		if len(manifestLayersInfo) == 0 {
			task.LogFatal(nil, "No layers found")
			return
		}
		topLayerID := manifestLayersInfo[len(manifestLayersInfo)-1].ID

		// 3. Get existing layer info
		layersInfo, err := getExistingPodmanLayerInfoFromCVMFS(cvmfsRepo)
		if err != nil {
			task.LogFatal(nil, fmt.Sprintf("Failed to get existing layer metadata: %s", err.Error()))
			return
		}
		task.Log(nil, db.LOG_SEVERITY_DEBUG, "Successfully got existing layer metadata")

		// 4. Append any new layers to the existing layers info, ignoring duplicates
		var newLayersInfo []PodmanLayerInfo
		layersInfo, newLayersInfo = updatePodmanLayerMetadata(layersInfo, manifestLayersInfo)
		if len(newLayersInfo) > 0 {
			task.Log(nil, db.LOG_SEVERITY_INFO, fmt.Sprintf("Imported %d new layers. %d were already present in the Podman store.", len(newLayersInfo), len(manifestLayersInfo)-len(newLayersInfo)))
			madeChanges = true

			// Update layers.json
			if err := updatePodmanLayerStoreCVMFS(layersInfo, cvmfsRepo); err != nil {
				task.LogFatal(nil, fmt.Sprintf("Failed to append to layers.json: %s", err.Error()))
				return
			}
			task.Log(nil, db.LOG_SEVERITY_DEBUG, "Updated layers.json")

			// We create files for the new layers here

			// Link rootfs for the new layers
			if err = LinkRootfsIntoPodmanStore(cvmfsRepo, newLayersInfo); err != nil {
				task.LogFatal(nil, fmt.Sprintf("Failed to link rootfs: %s", err.Error()))
				return
			}
			task.Log(nil, db.LOG_SEVERITY_DEBUG, "Linked rootfs for new layers")

			// Create links for the new layers
			if err = createLinks(cvmfsRepo, newLayersInfo); err != nil {
				task.LogFatal(nil, fmt.Sprintf("Failed to create links: %s", err.Error()))
				return
			}
			task.Log(nil, db.LOG_SEVERITY_DEBUG, "Created links for new layers")

			// Create lower files for the new layers
			if err := createLowerFilesCVMFS(cvmfsRepo, manifestLayersInfo, newLayersInfo); err != nil {
				task.LogFatal(nil, fmt.Sprintf("Failed to create lower files: %s", err.Error()))
				return
			}
			task.Log(nil, db.LOG_SEVERITY_DEBUG, "Created lower files")
		}

		// 5. Get existing images metadata
		imagesInfo, err := getExistingPodmanImageInfoFromCVMFS(cvmfsRepo)
		if err != nil {
			task.LogFatal(nil, fmt.Sprintf("Failed to get existing image metadata: %s", err.Error()))
			return
		}
		task.Log(nil, db.LOG_SEVERITY_DEBUG, "Successfully got existing image metadata")

		// 6. Update image metadata
		names := getPodmanNames(image, manifest)
		var changed bool
		var alreadyExists bool
		changed, alreadyExists, imagesInfo = updatePodmanImageNames(imagesInfo, manifest.ManifestDigest, names)
		if changed {
			task.Log(nil, db.LOG_SEVERITY_INFO, "Updated image names in podman store image metadata")
			madeChanges = true
		}
		if !alreadyExists {
			// Append the new image to the existing images info
			newImageInfo := createPodmanImageInfo(image, manifest, topLayerID)
			imagesInfo = append(imagesInfo, newImageInfo)
			task.Log(nil, db.LOG_SEVERITY_INFO, "Appended the new image to podman store image metadata")
			madeChanges = true
		}
		if madeChanges {
			// Write updated images.json
			if err := updatePodmanImageStoreCVMFS(imagesInfo, cvmfsRepo); err != nil {
				task.LogFatal(nil, fmt.Sprintf("Failed to update images.json: %s", err.Error()))
				return
			}
			task.Log(nil, db.LOG_SEVERITY_DEBUG, "Updated images.json")
		}

		// Create lock files, in case they don't already exist
		if changed, err = createLockFilesCVMFS(cvmfsRepo); err != nil {
			task.LogFatal(nil, fmt.Sprintf("Failed to create lock files: %s", err.Error()))
			return
		}
		if changed {
			task.Log(nil, db.LOG_SEVERITY_DEBUG, "Created lock files")
			madeChanges = true
		}

		// Create the manifest file
		if changed, err = createManifestFileCVMFS(manifest, cvmfsRepo); err != nil {
			task.LogFatal(nil, fmt.Sprintf("Failed to create manifest file: %s", err.Error()))
			return
		}
		if changed {
			madeChanges = true
			task.Log(nil, db.LOG_SEVERITY_DEBUG, "Created manifest file in Podman store")
		}

		// Create the config file
		if changed, err = createConfigFileCVMFS(imageConfig, manifest.ManifestDigest, cvmfsRepo); err != nil {
			task.LogFatal(nil, fmt.Sprintf("Failed to create config link: %s", err.Error()))
			return
		}
		if changed {
			madeChanges = true
			task.Log(nil, db.LOG_SEVERITY_DEBUG, "Created config file in Podman store")
		}

		if !madeChanges {
			task.Log(nil, db.LOG_SEVERITY_INFO, "No changes made to Podman store, skipping publish")
			task.SetTaskCompleted(nil, db.TASK_RESULT_SUCCESS)
			return
		}

		//  We have made changes, so we need to commit the transaction
		{
			// Stop the transaction from being aborted
			endTransactionOnce.Do(func() {})
			ok, stdOut, stdErr, err := cvmfs.PublishTransactionNew(cvmfsRepo)
			if err != nil {
				task.LogFatal(nil, fmt.Sprintf("Failed to commit transaction: %s", err.Error()))
				return
			} else if !ok {
				task.LogFatal(nil, fmt.Sprintf("Failed to commit transaction\nstdout:\n\n%s\nstderr:%s\n", stdOut, stdErr))
				return
			} else {
				task.Log(nil, db.LOG_SEVERITY_DEBUG, fmt.Sprintf("Successfully committed transaction\n%s", stdOut))
			}
		}

		task.Log(nil, db.LOG_SEVERITY_INFO, "Successfully published Podman image to CVMFS")
		task.SetTaskCompleted(nil, db.TASK_RESULT_SUCCESS)
	}()

	return ptr, nil
}

func getExistingPodmanLayerInfoFromCVMFS(cvmfsRepo string) ([]PodmanLayerInfo, error) {
	path := path.Join("/cvmfs", cvmfsRepo, config.PodmanSubDir, "overlay-layers", "layers.json")

	var layersInfo []PodmanLayerInfo

	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		// File does not exist, so we just return an empty list
		return layersInfo, nil
	} else if err != nil {
		return nil, err
	}
	layersInfoBytes, err := io.ReadAll(file)
	if err != nil {
		file.Close()
		return nil, err
	}
	file.Close()

	if err := json.Unmarshal(layersInfoBytes, &layersInfo); err != nil {
		return nil, err
	}

	return layersInfo, nil
}

// Reads from CVMFS so needs a lock
func getExistingPodmanImageInfoFromCVMFS(cvmfsRepo string) ([]PodmanImageInfo, error) {
	path := path.Join("/cvmfs", cvmfsRepo, config.PodmanSubDir, "overlay-images", "images.json")

	var imagesInfo []PodmanImageInfo

	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		// File does not exist, so we just return an empty list
		return imagesInfo, nil
	} else if err != nil {
		return nil, err
	}
	imagesInfoBytes, err := io.ReadAll(file)
	if err != nil {
		file.Close()
		return nil, err
	}
	file.Close()

	if err := json.Unmarshal(imagesInfoBytes, &imagesInfo); err != nil {
		return nil, err
	}

	return imagesInfo, nil
}

func updatePodmanImageStoreCVMFS(imagesInfo []PodmanImageInfo, cvmfsRepo string) error {
	path := path.Join("/cvmfs", cvmfsRepo, config.PodmanSubDir, "overlay-images", "images.json")

	file, err := os.OpenFile(path, os.O_TRUNC|os.O_CREATE|os.O_WRONLY, config.FilePermision)
	if err != nil {
		return fmt.Errorf("failed to open images.json: %w", err)
	}
	defer file.Close()

	updatedImagesInfoJSON, err := json.MarshalIndent(imagesInfo, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal images info: %w", err)
	}

	if _, err := file.Write(updatedImagesInfoJSON); err != nil {
		return fmt.Errorf("failed to write images info: %w", err)
	}
	return nil
}

func updatePodmanLayerStoreCVMFS(layersInfo []PodmanLayerInfo, cvmfsRepo string) error {
	path := path.Join("/cvmfs", cvmfsRepo, config.PodmanSubDir, "overlay-layers", "layers.json")

	file, err := os.OpenFile(path, os.O_TRUNC|os.O_CREATE|os.O_WRONLY, config.FilePermision)
	if err != nil {
		return fmt.Errorf("failed to open layers.json: %w", err)
	}
	defer file.Close()

	updatedLayersInfoJSON, err := json.MarshalIndent(layersInfo, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal layers info: %w", err)
	}

	if _, err := file.Write(updatedLayersInfoJSON); err != nil {
		return fmt.Errorf("failed to write layers info: %w", err)
	}

	return nil
}

func createLinks(cvmfsRepo string, layerInfo []PodmanLayerInfo) error {
	// Create the "l" directory if it does not exist
	linkdirPath := path.Join("/cvmfs", cvmfsRepo, config.PodmanSubDir, "overlay", "l")
	if err := os.MkdirAll(linkdirPath, config.DirPermision); err != nil {
		return err
	}
	for _, layer := range layerInfo {
		// Generate a random short id
		// Create the link
		linkPath := path.Join(linkdirPath, layer.ShortID)
		targetPath := filepath.Join("..", layer.ID, "diff")
		if err := os.Symlink(targetPath, linkPath); err != nil {
			return fmt.Errorf("failed to create link: %w", err)
		}
		// Create the link file
		overlayPath := path.Join("/cvmfs", cvmfsRepo, config.PodmanSubDir, "overlay", layer.ID)
		linkFilePath := path.Join(overlayPath, "link")
		// Create the overlay directory, in case this function is called before the overlay is created
		if err := os.MkdirAll(overlayPath, config.DirPermision); err != nil {
			return fmt.Errorf("failed to create overlay directory: %w", err)
		}
		if err := os.WriteFile(linkFilePath, []byte(layer.ShortID), config.FilePermision); err != nil {
			return fmt.Errorf("failed to write link file: %w", err)
		}
	}
	return nil
}

func LinkRootfsIntoPodmanStore(cvmfsRepo string, layersInfo []PodmanLayerInfo) error {
	podmanRootFSPath := path.Join("/cvmfs", cvmfsRepo, config.PodmanSubDir, "overlay")
	// Create the directory if it does not exist
	if err := os.MkdirAll(podmanRootFSPath, config.DirPermision); err != nil {
		return err
	}
	for _, layerInfo := range layersInfo {
		podmanLayerRootFSPath := path.Join(podmanRootFSPath, layerInfo.ID)
		// Create the layer directory
		if err := os.MkdirAll(podmanLayerRootFSPath, config.DirPermision); err != nil {
			return err
		}
		// Link "diff" to the actual layer
		targetPath := LayerRootfsPath(cvmfsRepo, layerInfo.CompressedDiffDigest)
		relativePath, err := filepath.Rel(podmanLayerRootFSPath, targetPath)
		if err != nil {
			return err
		}
		symlinkPath := path.Join(podmanLayerRootFSPath, "diff")
		// We don't care if there is a file here or not. We just remove it and create the symlink
		if err := os.Remove(symlinkPath); err != nil && !os.IsNotExist(err) {
			return err
		}
		if err := os.Symlink(relativePath, symlinkPath); err != nil {
			return err
		}
	}

	return nil
}

func createPodmanLayerInfo(manifest v1.Manifest, configObject v1.Image) ([]PodmanLayerInfo, error) {
	if len(manifest.Layers) != len(configObject.RootFS.DiffIDs) {
		return nil, fmt.Errorf("number of layers in manifest and config does not match")
	}
	out := make([]PodmanLayerInfo, len(manifest.Layers))

	var parentID digest.Digest
	for i, layer := range manifest.Layers {
		id, err := generateChainID(parentID, configObject.RootFS.DiffIDs[i])
		if err != nil {
			return nil, err
		}

		var parentIDStr string
		if parentID != "" {
			parentIDStr = parentID.Encoded()
		}

		out[i] = PodmanLayerInfo{
			ID:                   id.Encoded(),
			Parent:               parentIDStr,
			Created:              time.Now(),
			CompressedDiffDigest: layer.Digest,
			CompressedSize:       layer.Size,
			UncompressedDigest:   configObject.RootFS.DiffIDs[i],
		}
		out[i].ShortID, err = generatePodmanShortID()
		if err != nil {
			return nil, err
		}
		parentID = id
	}

	return out, nil
}

func getPodmanNames(image db.Image, manifest registry.ManifestWithBytesAndDigest) []string {
	// TODO: Do we want the digest name here?
	var names = make([]string, 0)
	if image.Tag != "" {
		names = append(names, fmt.Sprintf("%s/%s:%s", image.RegistryHost, image.Repository, image.Tag))
	}
	names = append(names, fmt.Sprintf("%s/%s@%s", image.RegistryHost, image.Repository, manifest.ManifestDigest))
	return names
}

func createPodmanImageInfo(image db.Image, manifest registry.ManifestWithBytesAndDigest, topLayerID string) PodmanImageInfo {
	return PodmanImageInfo{
		ID:      manifest.ManifestDigest.Encoded(),
		Names:   getPodmanNames(image, manifest),
		Layer:   topLayerID,
		Created: time.Now(),
	}
}

// Updates the names of the images in the existing image metadata if needed.
// Returns the updated metadata and a bool indicating if changes were made.
func updatePodmanImageNames(existingData []PodmanImageInfo, newImageDigest digest.Digest, newImageNames []string) (changed bool, imageExists bool, updatedData []PodmanImageInfo) {
	changed = false
	imageExists = false
	out := make([]PodmanImageInfo, 0, len(existingData))
	for _, existingImage := range existingData {
		if existingImage.ID == newImageDigest.Encoded() {
			// Image already exists, we might need to update the names
			imageExists = true
			combinedNames := removeDuplicateStrings(append(existingImage.Names, newImageNames...))
			if !reflect.DeepEqual(existingImage.Names, combinedNames) {
				// We have changed the names
				changed = true
				existingImage.Names = combinedNames
			}
			out = append(out, existingImage)
			continue
		}
		// An existing image with a different digest could have the same name.
		// This means that the existing image is outdated.
		// We solve this by removing any conflicting names from the existing image.
		newNames := differenceOfStringSets(existingImage.Names, newImageNames)
		if !reflect.DeepEqual(existingImage.Names, newNames) {
			// We have changed the names
			changed = true
			existingImage.Names = newNames
		}
		out = append(out, existingImage)
	}
	return changed, imageExists, out
}

func updatePodmanLayerMetadata(existingLayers []PodmanLayerInfo, NewLayers []PodmanLayerInfo) (updatedData []PodmanLayerInfo, addedLayers []PodmanLayerInfo) {
	existingMap := make(map[string]bool)
	for _, layer := range existingLayers {
		existingMap[layer.ID] = true
	}

	// Find the starting index from newData not in existingLayers
	startIndex := -1
	for idx, layer := range NewLayers {
		if !existingMap[layer.ID] {
			startIndex = idx
			break
		}
	}

	// If all newData layers are in existingLayers
	if startIndex == -1 {
		return existingLayers, nil
	}

	addedLayers = make([]PodmanLayerInfo, 0)
	updatedData = make([]PodmanLayerInfo, 0)

	updatedData = append(updatedData, existingLayers...)
	updatedData = append(updatedData, NewLayers[startIndex:]...)
	addedLayers = append(addedLayers, NewLayers[startIndex:]...)

	return updatedData, addedLayers
}
func removeDuplicateStrings(data []string) []string {
	// Create a copy of the input and then sort the copy
	dataCopy := make([]string, len(data))
	copy(dataCopy, data)
	sort.Strings(dataCopy)

	// Create the output slice
	out := make([]string, 0)

	// Iterate over the data copy and add to the output slice if the previous element is not equal
	for i, v := range dataCopy {
		// If it's the first element or if it's different from the previous one
		if i == 0 || v != dataCopy[i-1] {
			out = append(out, v)
		}
	}
	return out
}

func intersectionOfStringSets(a []string, b []string) []string {
	// Create copies of the inputs and then sort the copies
	aCopy := make([]string, len(a))
	bCopy := make([]string, len(b))
	copy(aCopy, a)
	copy(bCopy, b)
	sort.Strings(aCopy)
	sort.Strings(bCopy)

	// Create the output slice
	out := make([]string, 0)

	// Efficiently find the intersection using a two-pointer technique
	i, j := 0, 0
	for i < len(aCopy) && j < len(bCopy) {
		if aCopy[i] == bCopy[j] {
			// Avoid duplicates in the result
			if len(out) == 0 || out[len(out)-1] != aCopy[i] {
				out = append(out, aCopy[i])
			}
			i++
			j++
		} else if aCopy[i] < bCopy[j] {
			i++
		} else {
			j++
		}
	}
	return out
}

func differenceOfStringSets(a []string, b []string) []string {
	// Create copies of the inputs and then sort the copies
	aCopy := make([]string, len(a))
	bCopy := make([]string, len(b))
	copy(aCopy, a)
	copy(bCopy, b)
	sort.Strings(aCopy)
	sort.Strings(bCopy)

	// Create the output slice
	out := make([]string, 0)

	// Efficiently find the difference using a two-pointer technique
	i, j := 0, 0
	for i < len(aCopy) {
		if j < len(bCopy) && aCopy[i] == bCopy[j] {
			// Move forward in both slices since we're looking for non-matching strings
			i++
			j++
		} else if j >= len(bCopy) || aCopy[i] < bCopy[j] {
			// Avoid duplicates in the result
			if len(out) == 0 || out[len(out)-1] != aCopy[i] {
				out = append(out, aCopy[i])
			}
			i++
		} else {
			j++
		}
	}
	return out
}

// The short id can really be anything, as long as it is unique.
// There is a tradeoff between short and unique, though.
// Docker uses 26 characters from the set [A-Z0-9] when using the overlayfs driver
// so we do the same.
// https://docs.docker.com/storage/storagedriver/overlayfs-driver/
func generatePodmanShortID() (string, error) {
	const charset string = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	const length = 26
	out := make([]byte, length)

	maxInt := big.NewInt(int64(len(charset)))
	for i := range out {
		r, err := rand.Int(rand.Reader, maxInt)
		if err != nil {
			return "", err
		}
		out[i] = charset[r.Int64()]
	}
	return string(out), nil
}

// This uses Docker's style of generating the chain ID, by concatenating the bytes
// of the parent chain ID and the diff digest and then hashing the result.
func generateChainID(parentChainID digest.Digest, diffDigest digest.Digest) (digest.Digest, error) {
	if parentChainID == "" {
		return diffDigest, nil
	}
	// Convert the digests from hex to bytes
	parentChainIDBytes, err := hex.DecodeString(parentChainID.Encoded())
	if err != nil {
		return "", err
	}
	diffDigestBytes, err := hex.DecodeString(diffDigest.Encoded())
	if err != nil {
		return "", err
	}
	return digest.SHA256.FromBytes(append(parentChainIDBytes, diffDigestBytes...)), nil
}

func createLowerFilesCVMFS(cvmfsRepo string, manifestLayersInfo []PodmanLayerInfo, newLayersInfo []PodmanLayerInfo) error {
	lowerString := ""

	numExistingLayers := len(manifestLayersInfo) - len(newLayersInfo)
	if numExistingLayers > 0 {
		// We have existing layers, so start with the lower string for the last existing layer
		// Since layer short IDs are random, we need to read from CVMFS, not from the manifest
		existingLowerFile, err := os.Open(path.Join("/cvmfs", cvmfsRepo, config.PodmanSubDir, "overlay", manifestLayersInfo[numExistingLayers-1].ID, "lower"))
		if errors.Is(err, os.ErrNotExist) {
			// OK, the layer does not have a lower file, so we just start with an empty string
		} else if err != nil {
			return fmt.Errorf("failed to open existing lower file: %w", err)
		} else {
			if err != nil {
				return fmt.Errorf("failed to open existing lower file: %w", err)
			}
			existingLowerStringBytes, err := io.ReadAll(existingLowerFile)
			if err != nil {
				existingLowerFile.Close()
				return fmt.Errorf("failed to read existing lower file: %w", err)
			}
			existingLowerFile.Close()
			lowerString = string(existingLowerStringBytes)
		}

		// Append the last layer short ID to the lower string
		existingLinkFile, err := os.Open(path.Join("/cvmfs", cvmfsRepo, config.PodmanSubDir, "overlay", manifestLayersInfo[numExistingLayers-1].ID, "link"))
		if err != nil {
			return fmt.Errorf("failed to open existing link file: %w", err)
		}
		existingLinkStringBytes, err := io.ReadAll(existingLinkFile)
		if err != nil {
			existingLinkFile.Close()
			return fmt.Errorf("failed to read existing link file: %w", err)
		}
		existingLinkFile.Close()
		if lowerString == "" {
			lowerString = "l/" + string(existingLinkStringBytes)
		} else {
			lowerString = fmt.Sprintf("l/%s:%s", string(existingLinkStringBytes), lowerString)
		}
	}

	for _, layer := range newLayersInfo {
		if lowerString != "" {
			lowerFilePath := path.Join("/cvmfs", cvmfsRepo, config.PodmanSubDir, "overlay", layer.ID, "lower")
			file, err := os.OpenFile(lowerFilePath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, config.FilePermision)
			if err != nil {
				return fmt.Errorf("failed to create lower file: %w", err)
			}
			if _, err := file.WriteString(lowerString); err != nil {
				file.Close()
				return fmt.Errorf("failed to write lower file: %w", err)
			}
			file.Close()
		}

		// Update lowerString with this layer's ID
		if lowerString == "" {
			lowerString = "l/" + layer.ShortID
		} else {
			lowerString = fmt.Sprintf("l/%s:%s", layer.ShortID, lowerString)
		}

	}
	return nil
}

func createLockFilesCVMFS(cvmfsRepo string) (changed bool, err error) {
	layerLockPath := path.Join("/cvmfs", cvmfsRepo, config.PodmanSubDir, "overlay-layers", "layers.lock")
	imageLockPath := path.Join("/cvmfs", cvmfsRepo, config.PodmanSubDir, "overlay-images", "images.lock")

	file, err := os.OpenFile(layerLockPath, os.O_CREATE, config.FilePermision)
	if errors.Is(err, os.ErrExist) {
		// File already exists, we are good
	} else if err != nil {
		// Something else went wrong
		return changed, fmt.Errorf("failed to create layers.lock: %w", err)
	} else {
		// We just created the file
		file.Close()
		changed = true
	}

	file, err = os.OpenFile(imageLockPath, os.O_CREATE, config.FilePermision)
	if errors.Is(err, os.ErrExist) {
	} else if err != nil {
		return changed, fmt.Errorf("failed to create images.lock: %w", err)
	} else {
		file.Close()
		changed = true
	}

	return changed, nil
}

func createManifestFileCVMFS(manifest registry.ManifestWithBytesAndDigest, cvmfsRepo string) (changed bool, err error) {
	manifestPath := filepath.Join("/cvmfs", cvmfsRepo, config.PodmanSubDir, "overlay-images", manifest.ManifestDigest.Encoded(), "manifest")

	fileInfo, err := os.Stat(manifestPath)
	if err == nil {
		if fileInfo.Mode().IsRegular() {
			// File already exists, we are good
			return false, nil
		}
		// Previously, symlinks were used here. Let's remove it and create a new regular file.
		if err := os.Remove(manifestPath); err != nil {
			return false, fmt.Errorf("failed to remove existing manifest symlink: %w", err)
		}
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return false, fmt.Errorf("failed to stat manifest file: %w", err)
	}

	// Create the image directory if it doesn't exist
	err = os.MkdirAll(filepath.Dir(manifestPath), config.DirPermision)
	if err != nil {
		return true, fmt.Errorf("failed to create image directory: %w", err)
	}

	// Create the manifest file
	file, err := os.OpenFile(manifestPath, os.O_CREATE|os.O_WRONLY, config.FilePermision)
	if err != nil {
		return true, fmt.Errorf("failed to create manifest file: %w", err)
	}
	defer file.Close()

	// Write the manifest to the file
	if _, err := file.Write(manifest.ManifestBytes); err != nil {
		return true, fmt.Errorf("failed to write manifest file: %w", err)
	}

	return true, nil
}

func createConfigFileCVMFS(imageConfig registry.ConfigWithBytesAndDigest, imageDigest digest.Digest, cvmfsRepo string) (changed bool, err error) {
	imageConfigFilename := "=" + base64.StdEncoding.EncodeToString([]byte(imageConfig.ConfigDigest.String()))
	symlinkPath := filepath.Join("/cvmfs", cvmfsRepo, config.PodmanSubDir, "overlay-images", imageDigest.Encoded(), imageConfigFilename)

	fileInfo, err := os.Stat(symlinkPath)
	if err == nil {
		if fileInfo.Mode().IsRegular() {
			// File already exists, we are good
			return false, nil
		}
		// Previously, symlinks were used here. Let's remove it and create a new regular file.
		if err := os.Remove(symlinkPath); err != nil {
			return false, fmt.Errorf("failed to remove existing config symlink: %w", err)
		}
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return false, fmt.Errorf("failed to stat config file: %w", err)
	}

	// Create the image directory if it doesn't exist
	err = os.MkdirAll(filepath.Dir(symlinkPath), config.DirPermision)
	if err != nil {
		return true, fmt.Errorf("failed to create image directory: %w", err)
	}

	// Create the config file
	file, err := os.OpenFile(symlinkPath, os.O_CREATE|os.O_WRONLY, config.FilePermision)
	if err != nil {
		return true, fmt.Errorf("failed to create config file: %w", err)
	}
	defer file.Close()

	// Write the config to the file
	if _, err := file.Write(imageConfig.ConfigBytes); err != nil {
		return true, fmt.Errorf("failed to write config file: %w", err)
	}

	return true, nil
}
