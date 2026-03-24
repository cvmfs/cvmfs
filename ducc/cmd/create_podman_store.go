// create_podman_store.go implements the "create-podman-store" sub-command.
//
// It creates a local podman additional image store directory whose single
// overlay layer points directly to the pre-merged "flat" image that
// cvmfs_ducc stores under /cvmfs/<repo>/.flat/… on the read-only CVMFS
// mount.  Because the flat image already is the fully overlaid filesystem,
// we need only one synthetic layer – no overlay chain at container start-up.
//
// Store layout produced:
//
//	<store-dir>/
//	  overlay/
//	    <layerid>/
//	      diff    -> /cvmfs/<repo>/.flat/<xx>/<digest>/   (absolute symlink)
//	      link                                            (26-char random ID)
//	  overlay/l/
//	    <linkid> -> ../<layerid>/diff                    (relative symlink)
//	  overlay-images/
//	    images.json
//	    images.lock
//	    <imageid>/
//	      manifest                   (raw manifest JSON from registry)
//	      <configfilename>           (raw config blob JSON from registry)
//	  overlay-layers/
//	    layers.json
//	    layers.lock
//
// The store is suitable for use as a podman additionalImageStore (read-only).
// Add it to /etc/containers/storage.conf:
//
//	[storage.options.overlay]
//	additionalImageStores = ["<store-dir>"]
package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	"github.com/cvmfs/ducc/lib"
	l "github.com/cvmfs/ducc/log"
)

func init() {
	rootCmd.AddCommand(createPodmanStoreCmd)
}

var createPodmanStoreCmd = &cobra.Command{
	Use:   "create-podman-store <image> <cvmfs-repo> <store-dir>",
	Short: "Create a local podman additional image store backed by the CVMFS flat image",
	Long: `Creates a podman additional image store at <store-dir> that references the
pre-merged flat image for <image> stored under /cvmfs/<cvmfs-repo>/.flat/...

The flat image must already exist on the CVMFS repository (i.e. the image was
previously converted with "cvmfs_ducc convert-single-image" without --skip-flat).

Unlike the legacy podman store written into the repository itself, this store
lives on the local machine and uses a single synthetic overlay layer whose
diff/ directory is a symlink into the read-only CVMFS mount.  There is no
overlay merge at container start-up: the fully merged filesystem is served
directly from CVMFS.

The store directory can be placed anywhere on the local filesystem (e.g. under
/var/lib/containers/cvmfs-store/).  To make podman aware of it, add it to
/etc/containers/storage.conf:

  [storage.options.overlay]
  additionalImageStores = ["<store-dir>"]

Re-running the command for the same image is idempotent: the synthetic layer ID
is derived deterministically from the image config digest.`,
	Args: cobra.ExactArgs(3),
	RunE: func(cmd *cobra.Command, args []string) error {
		return createPodmanStore(args[0], args[1], args[2])
	},
}

func createPodmanStore(imageRef, cvmfsRepo, storeDir string) error {
	logger := l.Log()

	// ------------------------------------------------------------------ parse
	img, err := lib.ParseImage(imageRef)
	if err != nil {
		return fmt.Errorf("parsing image reference %q: %w", imageRef, err)
	}

	// ---------------------------------------------------------- fetch manifest
	manifest, err := img.GetManifest()
	if err != nil {
		return fmt.Errorf("fetching manifest for %q: %w", imageRef, err)
	}

	// The image ID used by containers/storage is the hex SHA256 of the config
	// blob digest (i.e. the part after "sha256:").
	imageID := strings.TrimPrefix(manifest.Config.Digest, "sha256:")

	// ------------------------------------------------- verify flat image exists
	// GetSingularityPath returns a repo-relative path like ".flat/ab/abcdef…"
	flatRelPath := manifest.GetSingularityPath()
	repoName := cvmfsRepo
	if idx := strings.Index(cvmfsRepo, "/"); idx != -1 {
		repoName = cvmfsRepo[:idx]
	}
	flatAbsPath := filepath.Join("/cvmfs", repoName, flatRelPath)

	if _, statErr := os.Stat(flatAbsPath); statErr != nil {
		return fmt.Errorf(
			"flat image not found at %s – run \"cvmfs_ducc convert-single-image\" without --skip-flat first: %w",
			flatAbsPath, statErr,
		)
	}

	logger.WithFields(log.Fields{
		"image":    imageRef,
		"imageID":  imageID,
		"flatPath": flatAbsPath,
		"store":    storeDir,
	}).Info("Creating podman store")

	// -------------------------------------------------- synthetic layer layout
	// Use a deterministic layer ID so re-running is idempotent.
	layerID := lib.FlatLayerID(imageID)

	overlayDir := filepath.Join(storeDir, "overlay")
	layerDir := filepath.Join(overlayDir, layerID)
	linkBaseDir := filepath.Join(overlayDir, "l")
	imagesMetaDir := filepath.Join(storeDir, "overlay-images")
	imageMetaDir := filepath.Join(imagesMetaDir, imageID)
	layersMetaDir := filepath.Join(storeDir, "overlay-layers")

	for _, dir := range []string{layerDir, linkBaseDir, imageMetaDir, layersMetaDir} {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return fmt.Errorf("creating directory %s: %w", dir, err)
		}
	}

	// overlay/<layerid>/diff -> flat image on CVMFS (absolute path)
	diffPath := filepath.Join(layerDir, "diff")
	if err := replaceSymlink(flatAbsPath, diffPath); err != nil {
		return fmt.Errorf("creating diff symlink: %w", err)
	}

	// overlay/<layerid>/link  (short random ID read by the l/ shortcut)
	linkFilePath := filepath.Join(layerDir, "link")
	// Re-use existing link ID if the layer dir already existed (idempotent).
	linkID, err := readOrGenerateLinkID(linkFilePath)
	if err != nil {
		return fmt.Errorf("creating link ID: %w", err)
	}

	// overlay/l/<linkID> -> ../<layerid>/diff  (relative symlink)
	linkSymPath := filepath.Join(linkBaseDir, linkID)
	linkTarget := filepath.Join("..", layerID, "diff")
	if err := replaceSymlink(linkTarget, linkSymPath); err != nil {
		return fmt.Errorf("creating l/%s symlink: %w", linkID, err)
	}

	// ---------------------------------------------- overlay-layers/layers.json
	flatSize, err := lib.DirSize(flatAbsPath)
	if err != nil {
		// Non-fatal: log and proceed with size 0; the store will still work
		// but podman may log a warning about unknown layer size.
		logger.WithField("error", err).Warn("Could not compute flat image size; diff-size will be 0")
		flatSize = 0
	}
	layerInfos := []lib.LayerInfo{{
		ID:               layerID,
		Created:          time.Now(),
		// UncompressedDigest must be non-empty or containers/image's getSize()
		// will error with "size for layer is unknown".  For the synthetic flat
		// layer there is no real uncompressed-tar digest, so we derive a stable
		// placeholder from the layer ID itself.
		UncompressedDigest: "sha256:" + layerID,
		UncompressedSize:   flatSize,
	}}
	layersJSON, err := json.MarshalIndent(layerInfos, "", " ")
	if err != nil {
		return fmt.Errorf("marshaling layers.json: %w", err)
	}
	if err := writeFile(filepath.Join(layersMetaDir, "layers.json"), layersJSON); err != nil {
		return fmt.Errorf("writing layers.json: %w", err)
	}

	// ---------------------------------------------- overlay-images/images.json
	imageInfos := []lib.ImageInfo{{
		ID:      imageID,
		Names:   []string{img.GetSimpleName()},
		Layer:   layerID,
		Created: time.Now(),
	}}
	imagesJSON, err := json.MarshalIndent(imageInfos, "", " ")
	if err != nil {
		return fmt.Errorf("marshaling images.json: %w", err)
	}
	if err := writeFile(filepath.Join(imagesMetaDir, "images.json"), imagesJSON); err != nil {
		return fmt.Errorf("writing images.json: %w", err)
	}

	// -------------------------------- overlay-images/<imageID>/manifest
	rawManifest, err := img.GetRawManifestBytes()
	if err != nil {
		return fmt.Errorf("fetching raw manifest: %w", err)
	}
	if err := writeFile(filepath.Join(imageMetaDir, "manifest"), rawManifest); err != nil {
		return fmt.Errorf("writing manifest file: %w", err)
	}

	// -------------------------------- overlay-images/<imageID>/<configfilename>
	// containers/storage escapes the config digest key into a safe filename.
	configFilename, err := lib.ConfigFileName(manifest.Config.Digest)
	if err != nil {
		return fmt.Errorf("computing config filename: %w", err)
	}
	if configFilename == "" {
		// Fallback (should not happen for sha256 digests): use raw digest
		// with colon replaced so the name is filesystem-safe.
		configFilename = strings.ReplaceAll(manifest.Config.Digest, ":", "-")
	}
	rawConfig, err := img.GetRawConfigBytes()
	if err != nil {
		return fmt.Errorf("fetching image config: %w", err)
	}
	if err := writeFile(filepath.Join(imageMetaDir, configFilename), rawConfig); err != nil {
		return fmt.Errorf("writing config file: %w", err)
	}

	// ------------------------------------------------------- lock files (empty)
	for _, lf := range []string{
		filepath.Join(imagesMetaDir, "images.lock"),
		filepath.Join(layersMetaDir, "layers.lock"),
	} {
		f, err := os.OpenFile(lf, os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return fmt.Errorf("creating lock file %s: %w", lf, err)
		}
		f.Close()
	}

	logger.WithField("store", storeDir).Info("Podman store created successfully")
	fmt.Printf("\nPodman store ready at: %s\n", storeDir)
	fmt.Printf("To use it, add to /etc/containers/storage.conf:\n")
	fmt.Printf("  [storage.options.overlay]\n")
	fmt.Printf("  additionalImageStores = [\"%s\"]\n\n", storeDir)
	fmt.Printf("Then pull the image with:\n")
	fmt.Printf("  podman pull %s\n", imageRef)
	return nil
}

// replaceSymlink atomically replaces (or creates) a symlink at name pointing
// to target.  It errors if name exists and is not a symlink.
func replaceSymlink(target, name string) error {
	if lstat, err := os.Lstat(name); err == nil {
		if lstat.Mode()&os.ModeSymlink != 0 {
			if err := os.Remove(name); err != nil {
				return fmt.Errorf("removing existing symlink %s: %w", name, err)
			}
		} else {
			return fmt.Errorf("%s already exists and is not a symlink", name)
		}
	}
	return os.Symlink(target, name)
}

// readOrGenerateLinkID reads the link ID from linkFilePath if it exists,
// otherwise generates a new 26-character random ID and writes it to the file.
func readOrGenerateLinkID(linkFilePath string) (string, error) {
	if data, err := os.ReadFile(linkFilePath); err == nil {
		id := strings.TrimSpace(string(data))
		if len(id) > 0 {
			return id, nil
		}
	}
	id, err := lib.GenerateID(26)
	if err != nil {
		return "", err
	}
	if err := writeFile(linkFilePath, []byte(id)); err != nil {
		return "", err
	}
	return id, nil
}

// writeFile writes data to path, creating parent directories as needed.
func writeFile(path string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	return os.WriteFile(path, data, 0644)
}

