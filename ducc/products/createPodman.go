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
	"time"

	"github.com/cvmfs/ducc/constants"
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
		task.Log(nil, db.LOG_SEVERITY_DEBUG, "Waiting for start")
		task.WaitForStart()
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
		config, ok := artifact.(registry.ConfigWithBytesAndDigest)
		if !ok {
			task.LogFatal(nil, fmt.Sprintf("Invalid config type: %s", reflect.TypeOf(artifact).String()))
			return
		}

		// 1. Ensure that podman directories and catalogs exist
		// TODO: Do we really want to create all these catalogs here?
		// Isn't it better to do this as some kind of initialization step?
		createCatalogIntoDirs := []string{
			constants.PodmanSubDir,
			path.Join(constants.PodmanSubDir, "overlay"),
			path.Join(constants.PodmanSubDir, "overlay-images"),
			path.Join(constants.PodmanSubDir, "overlay-layers")}
		for _, dir := range createCatalogIntoDirs {
			err = cvmfs.CreateCatalogIntoDir(cvmfsRepo, dir)
			if err != nil {
				task.LogFatal(nil, fmt.Sprintf("Failed to create catalog %s", dir))
				return
			}
		}
		task.Log(nil, db.LOG_SEVERITY_INFO, "Successfully created Podman metadata catalogs")

		// 2. Get existing images metadata
		task.Log(nil, db.LOG_SEVERITY_INFO, "Getting existing image metadata")
		existingImagesInfo, err := getExistingPodmanImageInfoFromCVMFS(cvmfsRepo)
		if err != nil {
			task.LogFatal(nil, fmt.Sprintf("Failed to get existing image metadata: %s", err.Error()))
			return
		}
		task.Log(nil, db.LOG_SEVERITY_INFO, "Successfully got existing image metadata")

		// 3. Check if image already exists, and correct names if needed
		names := getPodmanNames(image, manifest)
		updatedImagesInfo, alreadyExists := preparePodmanImageMetadata(existingImagesInfo, manifest.ManifestDigest, names)
		if alreadyExists {
			// Check if we have updated the image metadata
			if reflect.DeepEqual(existingImagesInfo, updatedImagesInfo) {
				// No changes
				task.Log(nil, db.LOG_SEVERITY_INFO, "Podman image already exists in cvmfs, and metadata is up to date. Skipping creation.")
				task.SetTaskCompleted(nil, db.TASK_RESULT_SUCCESS)
				return
			}
			cvmfsLock.Lock()
			defer cvmfsLock.Unlock()
			abort := false
			if err := cvmfs.WithinTransaction(cvmfsRepo, func() error {
				if err := updatePodmanImageStoreCVMFS(updatedImagesInfo, cvmfsRepo); err != nil {
					task.LogFatal(nil, fmt.Sprintf("Failed to update images.json: %s", err.Error()))
					abort = true
					return err
				}
				return nil
			}); err != nil {
				task.LogFatal(nil, "CVMFS transaction failed")
				return
			}
			if abort {
				task.LogFatal(nil, "Failed to update images.json")
				return
			}

			task.Log(nil, db.LOG_SEVERITY_INFO, "Podman image already exists in cvmfs, but metadata was outdated. Updated metadata.")
			task.SetTaskCompleted(nil, db.TASK_RESULT_SUCCESS)
			return
		}

		// 4. Create Podman layer metadata for the layers in the manifest
		manifestLayersInfo, err := createPodmanLayerInfo(manifest.Manifest, config.Config)
		if err != nil {
			task.LogFatal(nil, fmt.Sprintf("Failed to create layer metadata: %s", err.Error()))
			return
		}
		if len(manifestLayersInfo) == 0 {
			task.LogFatal(nil, "No layers found")
			return
		}
		topLayerID := manifestLayersInfo[len(manifestLayersInfo)-1].ID

		// 5. Get existing layer info
		existingLayersInfo, err := getExistingPodmanLayerInfoFromCVMFS(cvmfsRepo)
		if err != nil {
			task.LogFatal(nil, fmt.Sprintf("Failed to get existing layer metadata: %s", err.Error()))
			return
		}

		// 6. Append any new layers to the existing layers info, ignoring duplicates
		updatedLayersInfo, newLayersInfo := updatePodmanLayerMetadata(existingLayersInfo, manifestLayersInfo)
		task.Log(nil, db.LOG_SEVERITY_INFO, fmt.Sprintf("Out of the %d image layers, %d were already present in the Podman store", len(manifestLayersInfo), len(manifestLayersInfo)-len(newLayersInfo)))

		// 7. Create and append the new image to the existing images info
		newImageInfo := createPodmanImageInfo(image, manifest, topLayerID)
		updatedImagesInfo = append(updatedImagesInfo, newImageInfo)

		// Finally, write file changes to CVMFS, in one transaction
		abort := false // TODO: Make withinTransaction return if the transaction was aborted
		if err := cvmfs.WithinTransaction(cvmfsRepo, func() error {
			// Append to layers.json
			if err := updatePodmanLayerStoreCVMFS(updatedLayersInfo, cvmfsRepo); err != nil {
				task.LogFatal(nil, fmt.Sprintf("Failed to append to layers.json: %s", err.Error()))
				abort = true
				return err
			}
			task.Log(nil, db.LOG_SEVERITY_DEBUG, "Updated layers.json")

			// Write updated images.json
			if err := updatePodmanImageStoreCVMFS(updatedImagesInfo, cvmfsRepo); err != nil {
				task.LogFatal(nil, fmt.Sprintf("Failed to update images.json: %s", err.Error()))
				abort = true
				return err
			}
			task.Log(nil, db.LOG_SEVERITY_DEBUG, "Updated images.json")

			// Link rootfs for the new layers
			if err := LinkRootfsIntoPodmanStore(cvmfsRepo, newLayersInfo); err != nil {
				task.LogFatal(nil, fmt.Sprintf("Failed to link rootfs: %s", err.Error()))
				abort = true
				return err
			}
			task.Log(nil, db.LOG_SEVERITY_DEBUG, "Linked rootfs")

			// Create links for the new layers
			if err := createLinks(cvmfsRepo, newLayersInfo); err != nil {
				task.LogFatal(nil, fmt.Sprintf("Failed to create links: %s", err.Error()))
				abort = true
				return err
			}
			task.Log(nil, db.LOG_SEVERITY_DEBUG, "Created links")

			// Create lower files for the new layers
			if err := createLowerFilesCVMFS(cvmfsRepo, manifestLayersInfo, newLayersInfo); err != nil {
				task.LogFatal(nil, fmt.Sprintf("Failed to create lower files: %s", err.Error()))
				abort = true
				return err
			}
			task.Log(nil, db.LOG_SEVERITY_DEBUG, "Created lower files")

			// Create lock files, in case they don't already exist
			if err := createLockFilesCVMFS(cvmfsRepo); err != nil {
				task.LogFatal(nil, fmt.Sprintf("Failed to create lock files: %s", err.Error()))
				abort = true
				return err
			}
			task.Log(nil, db.LOG_SEVERITY_DEBUG, "Created lock files")

			// Create a link to the manifest file
			if err := linkManifestFileCVMFS(image, manifest.ManifestDigest, cvmfsRepo); err != nil {
				task.LogFatal(nil, fmt.Sprintf("Failed to create manifest link: %s", err.Error()))
				abort = true
				return err
			}
			task.Log(nil, db.LOG_SEVERITY_DEBUG, "Created manifest link")

			// Create a link to the config file
			if err := linkConfigFileCVMFS(image, manifest.Manifest.Config.Digest, manifest.ManifestDigest, cvmfsRepo); err != nil {
				task.LogFatal(nil, fmt.Sprintf("Failed to create config link: %s", err.Error()))
				abort = true
				return err
			}
			task.Log(nil, db.LOG_SEVERITY_DEBUG, "Created config link")

			return nil
		}); err != nil {
			task.LogFatal(nil, fmt.Sprintf("Failed to publish updated files to CVMFS: %s", err.Error()))
			return
		}
		if abort {
			task.LogFatal(nil, "CVMFS transaction was aborted due to previous errors")
			return
		}
		task.Log(nil, db.LOG_SEVERITY_INFO, "Successfully published Podman image to CVMFS")
		task.SetTaskCompleted(nil, db.TASK_RESULT_SUCCESS)
	}()

	return ptr, nil
}

func getExistingPodmanLayerInfoFromCVMFS(cvmfsRepo string) ([]PodmanLayerInfo, error) {
	path := path.Join("/cvmfs", cvmfsRepo, constants.PodmanSubDir, "overlay-layers", "layers.json")

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
	path := path.Join("/cvmfs", cvmfsRepo, constants.PodmanSubDir, "overlay-images", "images.json")

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
	path := path.Join("/cvmfs", cvmfsRepo, constants.PodmanSubDir, "overlay-images", "images.json")

	file, err := os.OpenFile(path, os.O_TRUNC|os.O_CREATE|os.O_WRONLY, constants.FilePermision)
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
	path := path.Join("/cvmfs", cvmfsRepo, constants.PodmanSubDir, "overlay-layers", "layers.json")

	file, err := os.OpenFile(path, os.O_TRUNC|os.O_CREATE|os.O_WRONLY, constants.FilePermision)
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
	linkdirPath := path.Join("/cvmfs", cvmfsRepo, constants.PodmanSubDir, "overlay", "l")
	if err := os.MkdirAll(linkdirPath, constants.DirPermision); err != nil {
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
		overlayPath := path.Join("/cvmfs", cvmfsRepo, constants.PodmanSubDir, "overlay", layer.ID)
		linkFilePath := path.Join(overlayPath, "link")
		// Create the overlay directory, in case this function is called before the overlay is created
		if err := os.MkdirAll(overlayPath, constants.DirPermision); err != nil {
			return fmt.Errorf("failed to create overlay directory: %w", err)
		}
		if err := os.WriteFile(linkFilePath, []byte(layer.ShortID), constants.FilePermision); err != nil {
			return fmt.Errorf("failed to write link file: %w", err)
		}
	}
	return nil
}

func LinkRootfsIntoPodmanStore(cvmfsRepo string, layersInfo []PodmanLayerInfo) error {
	podmanRootFSPath := path.Join("/cvmfs", cvmfsRepo, constants.PodmanSubDir, "overlay")
	// Create the directory if it does not exist
	if err := os.MkdirAll(podmanRootFSPath, constants.DirPermision); err != nil {
		return err
	}
	for _, layerInfo := range layersInfo {
		podmanLayerRootFSPath := path.Join(podmanRootFSPath, layerInfo.ID)
		// Create the layer directory
		if err := os.MkdirAll(podmanLayerRootFSPath, constants.DirPermision); err != nil {
			return err
		}
		// Link "diff" to the actual layer
		targetPath := cvmfs.LayerRootfsPath(cvmfsRepo, string(layerInfo.CompressedDiffDigest.Encoded()))
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

		/*// We create a second symlink to the overlay directory
		overlaySymlinkPath := filepath.Join("/cvmfs", cvmfsRepo, constants.PodmanSubDir, "overlay", layerInfo.ID)
		relativePath, err = filepath.Rel(filepath.Dir(overlaySymlinkPath), podmanLayerRootFSPath)
		if err != nil {
			return err
		}
		if err := os.Remove(overlaySymlinkPath); err != nil && !os.IsNotExist(err) {
			return err
		}
		if err := os.Symlink(relativePath, overlaySymlinkPath); err != nil {
			return err
		}*/
	}
	return nil
}

func createPodmanLayerInfo(manifest v1.Manifest, config v1.Image) ([]PodmanLayerInfo, error) {
	if len(manifest.Layers) != len(config.RootFS.DiffIDs) {
		return nil, fmt.Errorf("number of layers in manifest and config does not match")
	}
	out := make([]PodmanLayerInfo, len(manifest.Layers))

	var parentID digest.Digest
	for i, layer := range manifest.Layers {
		id, err := generateChainID(parentID, config.RootFS.DiffIDs[i])
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
			UncompressedDigest:   config.RootFS.DiffIDs[i],
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

func preparePodmanImageMetadata(existingData []PodmanImageInfo, newImageDigest digest.Digest, newImageNames []string) (updatedData []PodmanImageInfo, alreadyPresent bool) {
	alreadyPresent = false
	out := make([]PodmanImageInfo, 0)

	for _, existingImage := range existingData {
		if existingImage.ID == newImageDigest.Encoded() {
			alreadyPresent = true
			// Image already exists, update the names
			combinedNames := removeDuplicateStrings(append(existingImage.Names, newImageNames...))
			existingImage.Names = combinedNames
			out = append(out, existingImage)
			continue
		}
		// An existing image with a different digest could have the same name.
		// This means that the existing image is outdated.
		// We solve this by removing any conflicting names from the existing image.
		existingImage.Names = differenceOfStringSets(existingImage.Names, newImageNames)
		out = append(out, existingImage)
	}

	return out, alreadyPresent
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
		existingLowerFile, err := os.Open(path.Join("/cvmfs", cvmfsRepo, constants.PodmanSubDir, "overlay", manifestLayersInfo[numExistingLayers-1].ID, "lower"))
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
		existingLinkFile, err := os.Open(path.Join("/cvmfs", cvmfsRepo, constants.PodmanSubDir, "overlay", manifestLayersInfo[numExistingLayers-1].ID, "link"))
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
			lowerFilePath := path.Join("/cvmfs", cvmfsRepo, constants.PodmanSubDir, "overlay", layer.ID, "lower")
			file, err := os.OpenFile(lowerFilePath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, constants.FilePermision)
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

func createLockFilesCVMFS(cvmfsRepo string) error {
	layerLockPath := path.Join("/cvmfs", cvmfsRepo, constants.PodmanSubDir, "overlay-layers", "layers.lock")
	imageLockPath := path.Join("/cvmfs", cvmfsRepo, constants.PodmanSubDir, "overlay-images", "images.lock")

	file, err := os.OpenFile(layerLockPath, os.O_CREATE, constants.FilePermision)
	if errors.Is(err, os.ErrExist) {
		// File already exists, we are good
	} else if err != nil {
		// Something else went wrong
		return err
	} else {
		// We just created the file
		file.Close()
	}

	file, err = os.OpenFile(imageLockPath, os.O_CREATE, constants.FilePermision)
	if errors.Is(err, os.ErrExist) {
	} else if err != nil {
		return err
	} else {
		file.Close()
	}

	return nil
}

func linkManifestFileCVMFS(image db.Image, imageDigest digest.Digest, cvmfsRepo string) error {
	symlinkPath := filepath.Join("/cvmfs", cvmfsRepo, constants.PodmanSubDir, "overlay-images", imageDigest.Encoded(), "manifest")
	targetPath := filepath.Join("/cvmfs", cvmfsRepo, constants.ImagesSubDir, imageDigest.Encoded()[:2], imageDigest.Encoded(), "manifest.json")

	// Create the image directory if it doesn't exist
	err := os.MkdirAll(filepath.Dir(symlinkPath), constants.DirPermision)
	if err != nil {
		return err
	}

	relPath, err := filepath.Rel(filepath.Dir(symlinkPath), targetPath)
	if err != nil {
		return err
	}

	if err := os.Symlink(relPath, symlinkPath); err != nil {
		return err
	}

	return nil
}

func linkConfigFileCVMFS(image db.Image, configDigest digest.Digest, imageDigest digest.Digest, cvmfsRepo string) error {

	configFilename := "=" + base64.StdEncoding.EncodeToString([]byte(configDigest.String()))

	symlinkPath := filepath.Join("/cvmfs", cvmfsRepo, constants.PodmanSubDir, "overlay-images", imageDigest.Encoded(), configFilename)
	targetPath := filepath.Join("/cvmfs", cvmfsRepo, constants.ImagesSubDir, imageDigest.Encoded()[:2], imageDigest.Encoded(), "config.json")

	// Create the image directory if it doesn't exist
	err := os.MkdirAll(filepath.Dir(symlinkPath), constants.DirPermision)
	if err != nil {
		return err
	}

	relPath, err := filepath.Rel(filepath.Dir(symlinkPath), targetPath)
	if err != nil {
		return err
	}

	if err := os.Symlink(relPath, symlinkPath); err != nil {
		return err
	}

	return nil
}
