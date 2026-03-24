// podman_store.go defines the metadata types and the single-transaction
// function used to publish a podman additional image store into a CVMFS
// repository under podmanStore/.
//
// The store uses one synthetic overlay layer whose diff/ symlink points to the
// pre-merged flat image already present at .flat/<xx>/<digest>/ in the repo.
// All files are written within a single CVMFS transaction.

package lib

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	cvmfs "github.com/cvmfs/ducc/cvmfs"
	l "github.com/cvmfs/ducc/log"
	log "github.com/sirupsen/logrus"
)

// ImageInfo is a single entry in overlay-images/images.json.
type ImageInfo struct {
	ID      string    `json:"id,omitempty"`
	Names   []string  `json:"names,omitempty"`
	Layer   string    `json:"layer,omitempty"`
	Created time.Time `json:"created,omitempty"`
}

// LayerInfo is a single entry in overlay-layers/layers.json.
type LayerInfo struct {
	ID                   string    `json:"id,omitempty"`
	Parent               string    `json:"parent,omitempty"`
	Created              time.Time `json:"created,omitempty"`
	CompressedDiffDigest string    `json:"compressed-diff-digest,omitempty"`
	CompressedSize       int       `json:"compressed-size,omitempty"`
	UncompressedDigest   string    `json:"diff-digest,omitempty"`
	UncompressedSize     int64     `json:"diff-size,omitempty"`
}

const podmanStoreRoot = "podmanStore"

// FlatLayerID returns a deterministic layer ID for the synthetic flat layer.
// It is derived from the image config digest so it is stable across re-runs.
func FlatLayerID(imageID string) string {
	sum := sha256.Sum256([]byte("cvmfs-flat-layer:" + imageID))
	return hex.EncodeToString(sum[:])
}

// DirSize returns the total byte size of all regular files under root.
// Symlinks are not followed.
func DirSize(root string) (int64, error) {
	var total int64
	err := filepath.Walk(root, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			total += info.Size()
		}
		return nil
	})
	return total, err
}

// PublishPodmanStore writes the podman additional image store entry for img
// into /cvmfs/<repo>/podmanStore/ using a single CVMFS transaction.
// It requires that the flat image has already been published to the repo.
func (img *Image) PublishPodmanStore(CVMFSRepo string) error {
	return img.PublishPodmanStoreWithLogger(nil, CVMFSRepo)
}

func (img *Image) PublishPodmanStoreWithLogger(logger *log.Entry, CVMFSRepo string) error {
	logger = l.Ensure(logger)

	manifest, err := img.GetManifest()
	if err != nil {
		return fmt.Errorf("fetching manifest: %w", err)
	}

	imageID := strings.TrimPrefix(manifest.Config.Digest, "sha256:")
	repoName, _ := cvmfs.GetRepoAndSubdir(CVMFSRepo)
	storeRoot := filepath.Join("/cvmfs", repoName, podmanStoreRoot)

	// Idempotency check: skip if this imageID is already in images.json.
	imagesJSONPath := filepath.Join(storeRoot, "overlay-images", "images.json")
	existingImages := readImageInfos(imagesJSONPath)
	for _, e := range existingImages {
		if e.ID == imageID {
			logger.WithField("imageID", imageID).Trace("Image already present in podman store, skipping")
			return nil
		}
	}

	// Verify flat image is present in the repo.
	flatRelPath := manifest.GetSingularityPath()
	flatAbsPath := filepath.Join("/cvmfs", repoName, flatRelPath)
	if _, err := os.Stat(flatAbsPath); err != nil {
		return fmt.Errorf("flat image not found at %s (run convert without --skip-flat first): %w",
			flatAbsPath, err)
	}

	layerID := FlatLayerID(imageID)

	// Read existing layers.json before the transaction.
	layersJSONPath := filepath.Join(storeRoot, "overlay-layers", "layers.json")
	existingLayers := readLayerInfos(layersJSONPath)

	// Compute flat-image size (used as diff-size in layers.json).
	flatSize, err := DirSize(flatAbsPath)
	if err != nil {
		logger.WithField("error", err).Warn("Could not compute flat image size; diff-size will be 0")
	}

	// Read or generate the link ID (stable across re-runs of the same layer).
	linkFilePath := filepath.Join(storeRoot, "overlay", layerID, "link")
	linkID := readLinkIDFromFile(linkFilePath)
	if linkID == "" {
		linkID, err = GenerateID(26)
		if err != nil {
			return fmt.Errorf("generating link ID: %w", err)
		}
	}

	// Fetch config and raw manifest bytes before the transaction.
	rawManifest, err := img.GetRawManifestBytes()
	if err != nil {
		return fmt.Errorf("fetching raw manifest: %w", err)
	}
	rawConfig, err := img.GetRawConfigBytes()
	if err != nil {
		return fmt.Errorf("fetching config: %w", err)
	}
	configFilename, err := ConfigFileName(manifest.Config.Digest)
	if err != nil {
		return fmt.Errorf("computing config filename: %w", err)
	}

	// Build merged layers.json and images.json entries.
	newLayer := LayerInfo{
		ID:                 layerID,
		Created:            time.Now(),
		UncompressedDigest: "sha256:" + layerID,
		UncompressedSize:   flatSize,
	}
	mergedLayers := append(existingLayers, newLayer)

	newImage := ImageInfo{
		ID:      imageID,
		Names:   []string{img.GetSimpleName()},
		Layer:   layerID,
		Created: time.Now(),
	}
	mergedImages := append(existingImages, newImage)

	layersJSON, err := json.MarshalIndent(mergedLayers, "", " ")
	if err != nil {
		return fmt.Errorf("marshaling layers.json: %w", err)
	}
	imagesJSON, err := json.MarshalIndent(mergedImages, "", " ")
	if err != nil {
		return fmt.Errorf("marshaling images.json: %w", err)
	}

	// Single CVMFS transaction: create all store files and symlinks at once.
	return cvmfs.WithinTransactionWithLogger(logger, CVMFSRepo, func() error {
		dirs := []string{
			filepath.Join(storeRoot, "overlay", layerID),
			filepath.Join(storeRoot, "overlay", "l"),
			filepath.Join(storeRoot, "overlay-images", imageID),
			filepath.Join(storeRoot, "overlay-layers"),
		}
		for _, d := range dirs {
			if err := os.MkdirAll(d, 0755); err != nil {
				return fmt.Errorf("creating %s: %w", d, err)
			}
		}

		// diff -> relative path from podmanStore/overlay/<layerID>/ to the flat image.
		// e.g. "../../../.flat/a4/<digest>"
		diffPath := filepath.Join(storeRoot, "overlay", layerID, "diff")
		if _, err := os.Lstat(diffPath); os.IsNotExist(err) {
			diffTarget := filepath.Join("..", "..", "..", flatRelPath)
			if err := os.Symlink(diffTarget, diffPath); err != nil {
				return fmt.Errorf("creating diff symlink: %w", err)
			}
		}

		// link file
		if _, err := os.Lstat(linkFilePath); os.IsNotExist(err) {
			if err := os.WriteFile(linkFilePath, []byte(linkID), 0644); err != nil {
				return fmt.Errorf("writing link file: %w", err)
			}
		}

		// overlay/l/<linkID> -> ../<layerID>/diff
		lSymPath := filepath.Join(storeRoot, "overlay", "l", linkID)
		if _, err := os.Lstat(lSymPath); os.IsNotExist(err) {
			if err := os.Symlink(filepath.Join("..", layerID, "diff"), lSymPath); err != nil {
				return fmt.Errorf("creating l/%s symlink: %w", linkID, err)
			}
		}

		// layers.json (always rewritten to include the new entry)
		if err := os.WriteFile(layersJSONPath, layersJSON, 0644); err != nil {
			return fmt.Errorf("writing layers.json: %w", err)
		}

		// images.json (always rewritten)
		if err := os.WriteFile(imagesJSONPath, imagesJSON, 0644); err != nil {
			return fmt.Errorf("writing images.json: %w", err)
		}

		// overlay-images/<imageID>/manifest
		manifestFilePath := filepath.Join(storeRoot, "overlay-images", imageID, "manifest")
		if _, err := os.Lstat(manifestFilePath); os.IsNotExist(err) {
			if err := os.WriteFile(manifestFilePath, rawManifest, 0644); err != nil {
				return fmt.Errorf("writing manifest: %w", err)
			}
		}

		// overlay-images/<imageID>/<configFilename>
		configFilePath := filepath.Join(storeRoot, "overlay-images", imageID, configFilename)
		if _, err := os.Lstat(configFilePath); os.IsNotExist(err) {
			if err := os.WriteFile(configFilePath, rawConfig, 0644); err != nil {
				return fmt.Errorf("writing config: %w", err)
			}
		}

		// Empty lock files (required by containers/storage).
		for _, lf := range []string{
			filepath.Join(storeRoot, "overlay-images", "images.lock"),
			filepath.Join(storeRoot, "overlay-layers", "layers.lock"),
		} {
			if _, err := os.Lstat(lf); os.IsNotExist(err) {
				f, err := os.OpenFile(lf, os.O_CREATE|os.O_WRONLY, 0644)
				if err != nil {
					return fmt.Errorf("creating %s: %w", lf, err)
				}
				f.Close()
			}
		}

		return nil
	})
}

// readImageInfos reads images.json from path, returning nil slice on any error.
func readImageInfos(path string) []ImageInfo {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	var infos []ImageInfo
	json.Unmarshal(data, &infos)
	return infos
}

// readLayerInfos reads layers.json from path, returning nil slice on any error.
func readLayerInfos(path string) []LayerInfo {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	var infos []LayerInfo
	json.Unmarshal(data, &infos)
	return infos
}

// readLinkIDFromFile returns the link ID stored at path, or "" if absent.
func readLinkIDFromFile(path string) string {
	data, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(data))
}
