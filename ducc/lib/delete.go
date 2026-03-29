package lib

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	cvmfs "github.com/cvmfs/ducc/cvmfs"
	l "github.com/cvmfs/ducc/log"
	log "github.com/sirupsen/logrus"
)

// DeleteImageFromCVMFS deletes the user-facing symlink and manifest directory
// for the given image from the CVMFS repository, making it eligible for
// garbage collection on the next run. Returns true if any paths existed and
// were (or would be) deleted.
func DeleteImageFromCVMFS(CVMFSRepo string, img *Image, dryRun bool) (bool, error) {
	symlinkPath := filepath.Join("/", "cvmfs", CVMFSRepo, img.GetPublicSymlinkPath())
	manifestDir := filepath.Join("/", "cvmfs", CVMFSRepo, ".metadata", img.GetSimpleName())
	multiarchBase := filepath.Join("/", "cvmfs", CVMFSRepo, ".multiarch")
	metadataBase := filepath.Join("/", "cvmfs", CVMFSRepo, ".metadata")

	_, symlinkErr := os.Lstat(symlinkPath)
	_, manifestErr := os.Stat(manifestDir)

	symlinkExists := symlinkErr == nil
	manifestExists := manifestErr == nil
	multiarchPaths := findMultiarchImagePaths(multiarchBase, img)
	multiarchMetadataDirs := findMultiarchMetadataDirs(metadataBase, img)

	if !symlinkExists && !manifestExists && len(multiarchPaths) == 0 && len(multiarchMetadataDirs) == 0 {
		l.Log().WithFields(log.Fields{"image": img.GetSimpleName()}).
			Info("Image not found in CVMFS, nothing to delete")
		return false, nil
	}

	if dryRun {
		if symlinkExists {
			fmt.Printf("[dry-run] Would delete symlink: %s\n", symlinkPath)
		}
		if manifestExists {
			fmt.Printf("[dry-run] Would delete manifest dir: %s\n", manifestDir)
		}
		for _, p := range multiarchPaths {
			fmt.Printf("[dry-run] Would delete .multiarch symlink: %s\n", p)
		}
		for _, p := range multiarchMetadataDirs {
			fmt.Printf("[dry-run] Would delete .metadata/.multiarch manifest dir: %s\n", p)
		}
		return true, nil
	}

	err := cvmfs.WithinTransaction(CVMFSRepo, func() error {
		if symlinkExists {
			if err := os.Remove(symlinkPath); err != nil && !os.IsNotExist(err) {
				return fmt.Errorf("error removing symlink %s: %w", symlinkPath, err)
			}
		}
		if manifestExists {
			if err := os.RemoveAll(manifestDir); err != nil {
				return fmt.Errorf("error removing manifest dir %s: %w", manifestDir, err)
			}
		}
		for _, p := range multiarchPaths {
			if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
				return fmt.Errorf("error removing .multiarch symlink %s: %w", p, err)
			}
		}
		for _, p := range multiarchMetadataDirs {
			if err := os.RemoveAll(p); err != nil && !os.IsNotExist(err) {
				return fmt.Errorf("error removing .metadata/.multiarch manifest dir %s: %w", p, err)
			}
		}
		return nil
	})
	if err != nil {
		return true, err
	}

	l.Log().WithFields(log.Fields{"image": img.GetSimpleName()}).
		Info("Deleted image from CVMFS")
	return true, nil
}

// findMultiarchImagePaths returns the absolute paths of per-image symlinks
// that exist inside the .multiarch directory tree for the given image.
//
// Conversion places a symlink for each architecture at:
//
//	.multiarch/<arch>/<registry>/<repo>:<tag>
//
// This function scans the top-level entries of multiarchBase, skips alias
// symlinks (e.g. aarch64 → arm64) and entries that are not directories, and
// returns every path of the form <archDir>/<img.GetPublicSymlinkPath()> that
// actually exists.
func findMultiarchImagePaths(multiarchBase string, img *Image) []string {
	entries, err := ioutil.ReadDir(multiarchBase)
	if err != nil {
		// .multiarch does not exist or is unreadable — nothing to clean up.
		return nil
	}

	relPath := img.GetPublicSymlinkPath() // <registry>/<repo>:<tag>
	var result []string
	for _, entry := range entries {
		// Skip alias symlinks (e.g. aarch64 → arm64:v8); they are repo-global
		// and must not be removed on a per-image basis.
		if entry.Mode()&os.ModeSymlink != 0 {
			continue
		}
		if !entry.IsDir() {
			continue
		}
		p := filepath.Join(multiarchBase, entry.Name(), relPath)
		if _, err := os.Lstat(p); err == nil {
			result = append(result, p)
		}
	}
	return result
}

// findMultiarchMetadataDirs returns the absolute paths of per-architecture
// manifest directories for the given image inside the .metadata tree.
//
// During conversion, a manifest is stored for each platform at:
//
//	.metadata/.multiarch/<arch>/<simpleName>/manifest.json
//
// This function scans the top-level entries of .metadata/.multiarch/, skips
// alias symlinks (e.g. aarch64 → arm64) and non-directory entries, and
// returns every directory of the form <archDir>/<img.GetSimpleName()> that
// actually exists.
func findMultiarchMetadataDirs(metadataBase string, img *Image) []string {
	multiarchMetaBase := filepath.Join(metadataBase, ".multiarch")
	entries, err := ioutil.ReadDir(multiarchMetaBase)
	if err != nil {
		// .metadata/.multiarch does not exist or is unreadable — nothing to clean up.
		return nil
	}

	simpleName := img.GetSimpleName() // <registry>/<repo>:<tag>
	var result []string
	for _, entry := range entries {
		// Skip alias symlinks (e.g. aarch64 → arm64); they point to other arch
		// directories and must not be removed.
		if entry.Mode()&os.ModeSymlink != 0 {
			continue
		}
		if !entry.IsDir() {
			continue
		}
		p := filepath.Join(multiarchMetaBase, entry.Name(), simpleName)
		if _, err := os.Stat(p); err == nil {
			result = append(result, p)
		}
	}
	return result
}

// ListAllCVMFSImages returns all images currently stored under
// /cvmfs/<CVMFSRepo>/.metadata by walking the registry/repository/tag
// directory hierarchy.  The .multiarch sub-directory is skipped.
func ListAllCVMFSImages(CVMFSRepo string) ([]*Image, error) {
	metadataBase := filepath.Join("/", "cvmfs", CVMFSRepo, ".metadata")

	registries, err := ioutil.ReadDir(metadataBase)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to read metadata directory %s: %w", metadataBase, err)
	}

	var images []*Image
	for _, entry := range registries {
		if !entry.IsDir() || entry.Name() == ".multiarch" {
			continue
		}
		registry := entry.Name()
		registryDir := filepath.Join(metadataBase, registry)
		found, err := listImagesUnderDir(registryDir, registry, "")
		if err != nil {
			l.LogE(err).WithFields(log.Fields{"registry": registry}).
				Warning("Error listing images for registry, skipping")
			continue
		}
		images = append(images, found...)
	}
	return images, nil
}

// listImagesUnderDir recursively walks dir, collecting entries whose name
// contains a colon (interpreted as "<repo-basename>:<tag>" image directories).
// Entries without a colon are treated as repository path components and are
// descended into, with their name appended to repoPrefix.
func listImagesUnderDir(dir, registry, repoPrefix string) ([]*Image, error) {
	entries, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	var images []*Image
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		name := entry.Name()
		if idx := strings.Index(name, ":"); idx != -1 {
			// "<basename>:<tag>" — this directory represents an image.
			repoBasename := name[:idx]
			tag := name[idx+1:]
			repository := repoBasename
			if repoPrefix != "" {
				repository = repoPrefix + "/" + repoBasename
			}
			imageURL := fmt.Sprintf("https://%s/%s:%s", registry, repository, tag)
			img, parseErr := ParseImage(imageURL)
			if parseErr != nil {
				l.LogE(parseErr).WithFields(log.Fields{"imageURL": imageURL}).
					Warning("Failed to parse image URL from CVMFS metadata, skipping")
				continue
			}
			images = append(images, &img)
		} else {
			// Intermediate repository path component — recurse.
			newPrefix := name
			if repoPrefix != "" {
				newPrefix = repoPrefix + "/" + name
			}
			subImages, err := listImagesUnderDir(filepath.Join(dir, name), registry, newPrefix)
			if err != nil {
				l.LogE(err).WithFields(log.Fields{"dir": filepath.Join(dir, name)}).
					Warning("Error walking metadata subdirectory, skipping")
				continue
			}
			images = append(images, subImages...)
		}
	}
	return images, nil
}

// FindCVMFSImagesMatchingPattern scans the .metadata directory and returns
// images whose registry, repository and tag (matched against tagPattern glob)
// correspond to an entry that has been converted into the CVMFS repository.
func FindCVMFSImagesMatchingPattern(CVMFSRepo, registry, repository, tagPattern string) ([]*Image, error) {
	repoBase := filepath.Base(repository)
	repoParent := filepath.Dir(repository)

	searchDir := filepath.Join("/", "cvmfs", CVMFSRepo, ".metadata", registry)
	if repoParent != "." {
		searchDir = filepath.Join(searchDir, repoParent)
	}

	entries, err := ioutil.ReadDir(searchDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	var result []*Image
	prefix := repoBase + ":"
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasPrefix(name, prefix) {
			continue
		}
		tag := strings.TrimPrefix(name, prefix)

		matched, err := filepath.Match(tagPattern, tag)
		if err != nil || !matched {
			continue
		}

		imageURL := fmt.Sprintf("https://%s/%s:%s", registry, repository, tag)
		img, err := ParseImage(imageURL)
		if err != nil {
			l.LogE(err).WithFields(log.Fields{"imageURL": imageURL}).
				Warning("Failed to parse image URL from CVMFS metadata, skipping")
			continue
		}
		result = append(result, &img)
	}
	return result, nil
}

// ExistsOnRegistry checks whether the image still exists on its registry by
// performing a HEAD request against the manifest URL and verifying a 2xx status.
func (img *Image) ExistsOnRegistry() bool {
	url := img.GetManifestUrl("")
	token, err := firstRequestForAuth(url)
	if err != nil {
		l.LogE(err).WithFields(log.Fields{"image": img.GetSimpleName()}).
			Warning("Failed to obtain auth token for registry existence check")
		return false
	}

	client := &http.Client{}
	req, err := http.NewRequest("HEAD", url, nil)
	if err != nil {
		return false
	}
	req.Header.Set("Authorization", token)
	req.Header.Set("Accept",
		"application/vnd.docker.distribution.manifest.v2+json, application/vnd.oci.image.manifest.v1+json")

	resp, err := client.Do(req)
	if err != nil {
		return false
	}
	resp.Body.Close()
	return resp.StatusCode >= 200 && resp.StatusCode < 300
}

// collectImageDeletePaths returns all CVMFS paths that should be removed for
// the given image (symlinks, manifest dirs, multiarch entries).  The returned
// paths are relative to the repository root (i.e. stripped of the
// /cvmfs/<repo> prefix) so they can be passed directly to
// cvmfs_server ingest --fast-delete.
func collectImageDeletePaths(CVMFSRepo string, img *Image) []string {
	repoRoot := filepath.Join("/", "cvmfs", CVMFSRepo)
	symlinkPath := filepath.Join(repoRoot, img.GetPublicSymlinkPath())
	manifestDir := filepath.Join(repoRoot, ".metadata", img.GetSimpleName())
	multiarchBase := filepath.Join(repoRoot, ".multiarch")
	metadataBase := filepath.Join(repoRoot, ".metadata")

	var paths []string

	if _, err := os.Lstat(symlinkPath); err == nil {
		paths = append(paths, strings.TrimPrefix(symlinkPath, repoRoot+"/"))
	}
	if _, err := os.Stat(manifestDir); err == nil {
		paths = append(paths, strings.TrimPrefix(manifestDir, repoRoot+"/"))
	}
	for _, p := range findMultiarchImagePaths(multiarchBase, img) {
		paths = append(paths, strings.TrimPrefix(p, repoRoot+"/"))
	}
	for _, p := range findMultiarchMetadataDirs(metadataBase, img) {
		paths = append(paths, strings.TrimPrefix(p, repoRoot+"/"))
	}
	return paths
}

// PruneImages removes from the CVMFS repository all images whose simple name
// (registry/repository:tag) is not present in desiredImages.  It lists every
// image currently stored under /cvmfs/<CVMFSRepo>, collects all paths that
// need to be removed, and deletes them in batches using
// `cvmfs_server ingest --fast-delete`.
//
// desiredImages must be the fully-expanded set of images derived from all
// wishlists (i.e. wildcard patterns already resolved against the registry),
// keyed by Image.GetSimpleName().
func PruneImages(CVMFSRepo string, desiredImages map[string]bool, dryRun bool) (int, error) {
	cvmfsImages, err := ListAllCVMFSImages(CVMFSRepo)
	if err != nil {
		return 0, fmt.Errorf("failed to list images in CVMFS: %w", err)
	}

	// Collect all paths to delete and count affected images.
	var allPaths []string
	deleted := 0
	for _, img := range cvmfsImages {
		if desiredImages[img.GetSimpleName()] {
			continue
		}
		paths := collectImageDeletePaths(CVMFSRepo, img)
		if len(paths) == 0 {
			l.Log().WithFields(log.Fields{"image": img.GetSimpleName()}).
				Info("Image not found in CVMFS, nothing to delete")
			continue
		}

		l.Log().WithFields(log.Fields{"image": img.GetSimpleName(), "paths": len(paths)}).
			Info("Image not in expanded wishlist, scheduling for deletion")

		if dryRun {
			for _, p := range paths {
				fmt.Printf("[dry-run] Would fast-delete: %s\n", p)
			}
		}

		allPaths = append(allPaths, paths...)
		deleted++
	}

	if dryRun || len(allPaths) == 0 {
		return deleted, nil
	}

	// Batch the paths into commands (up to 50 --fast-delete flags per
	// invocation to avoid exceeding argument-length limits).
	const pathsPerBatch = 50
	for i := 0; i < len(allPaths); i += pathsPerBatch {
		end := i + pathsPerBatch
		if end > len(allPaths) {
			end = len(allPaths)
		}
		batch := allPaths[i:end]

		l.Log().WithFields(log.Fields{
			"batch_size": len(batch),
			"batch_start": i,
			"total_paths": len(allPaths),
		}).Info("Executing ingest --fast-delete batch")

		if err := cvmfs.IngestFastDelete(CVMFSRepo, batch); err != nil {
			return deleted, fmt.Errorf("ingest --fast-delete failed at batch starting at index %d: %w", i, err)
		}
	}

	return deleted, nil
}
