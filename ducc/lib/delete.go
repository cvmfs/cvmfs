package lib

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"

	cvmfs "github.com/cvmfs/ducc/cvmfs"
	l "github.com/cvmfs/ducc/log"
	log "github.com/sirupsen/logrus"
)

// imageCVMFSPaths holds the paths in the CVMFS repository that belong to a
// single image and that exist at the time of the lookup.
type imageCVMFSPaths struct {
	symlink               string
	manifestDir           string
	multiarchPaths        []string
	multiarchMetadataDirs []string
}

func findImageCVMFSPaths(CVMFSRepo string, img *Image) imageCVMFSPaths {
	symlinkPath := filepath.Join("/", "cvmfs", CVMFSRepo, img.GetPublicSymlinkPath())
	manifestDir := filepath.Join("/", "cvmfs", CVMFSRepo, ".metadata", img.GetSimpleName())
	multiarchBase := filepath.Join("/", "cvmfs", CVMFSRepo, ".multiarch")
	metadataBase := filepath.Join("/", "cvmfs", CVMFSRepo, ".metadata")

	var p imageCVMFSPaths
	if _, err := os.Lstat(symlinkPath); err == nil {
		p.symlink = symlinkPath
	}
	if _, err := os.Stat(manifestDir); err == nil {
		p.manifestDir = manifestDir
	}
	p.multiarchPaths = findMultiarchImagePaths(multiarchBase, img)
	p.multiarchMetadataDirs = findMultiarchMetadataDirs(metadataBase, img)
	return p
}

func (p imageCVMFSPaths) empty() bool {
	return p.symlink == "" && p.manifestDir == "" &&
		len(p.multiarchPaths) == 0 && len(p.multiarchMetadataDirs) == 0
}

func (p imageCVMFSPaths) printDryRun() {
	if p.symlink != "" {
		fmt.Printf("[dry-run] Would delete symlink: %s\n", p.symlink)
	}
	if p.manifestDir != "" {
		fmt.Printf("[dry-run] Would delete manifest dir: %s\n", p.manifestDir)
	}
	for _, path := range p.multiarchPaths {
		fmt.Printf("[dry-run] Would delete .multiarch symlink: %s\n", path)
	}
	for _, path := range p.multiarchMetadataDirs {
		fmt.Printf("[dry-run] Would delete .metadata/.multiarch manifest dir: %s\n", path)
	}
}

// remove deletes the paths; it must run inside an open transaction.
func (p imageCVMFSPaths) remove() error {
	if p.symlink != "" {
		if err := os.Remove(p.symlink); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("error removing symlink %s: %w", p.symlink, err)
		}
	}
	if p.manifestDir != "" {
		if err := os.RemoveAll(p.manifestDir); err != nil {
			return fmt.Errorf("error removing manifest dir %s: %w", p.manifestDir, err)
		}
	}
	for _, path := range p.multiarchPaths {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("error removing .multiarch symlink %s: %w", path, err)
		}
	}
	for _, path := range p.multiarchMetadataDirs {
		if err := os.RemoveAll(path); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("error removing .metadata/.multiarch manifest dir %s: %w", path, err)
		}
	}
	return nil
}

// DeleteImageFromCVMFS deletes the user-facing symlink and manifest directory
// for the given image from the CVMFS repository, making it eligible for
// garbage collection on the next run. Returns true if any paths existed and
// were (or would be) deleted.
func DeleteImageFromCVMFS(CVMFSRepo string, img *Image, dryRun bool) (bool, error) {
	paths := findImageCVMFSPaths(CVMFSRepo, img)
	if paths.empty() {
		l.Log().WithFields(log.Fields{"image": img.GetSimpleName()}).
			Info("Image not found in CVMFS, nothing to delete")
		return false, nil
	}

	if dryRun {
		paths.printDryRun()
		return true, nil
	}

	if err := cvmfs.WithinTransaction(CVMFSRepo, paths.remove); err != nil {
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

// FindAllCVMFSImages scans the .metadata directory and returns every image
// that has been converted into the CVMFS repository. Images converted from a
// manifest list only have per-architecture manifests under
// .metadata/.multiarch/<arch>/, so that tree is scanned as well; each image is
// returned once regardless of how many architectures it has.
func FindAllCVMFSImages(CVMFSRepo string) ([]*Image, error) {
	metadataRoot := filepath.Join("/", "cvmfs", CVMFSRepo, ".metadata")
	if _, err := os.Stat(metadataRoot); os.IsNotExist(err) {
		return nil, nil
	}

	seen := make(map[string]bool)
	var result []*Image
	add := func(img *Image) {
		name := img.GetSimpleName()
		if seen[name] {
			return
		}
		seen[name] = true
		result = append(result, img)
	}

	multiarchRoot := filepath.Join(metadataRoot, ".multiarch")
	imgs, err := findImagesBelow(metadataRoot, multiarchRoot)
	if err != nil {
		return nil, err
	}
	for _, img := range imgs {
		add(img)
	}

	entries, err := ioutil.ReadDir(multiarchRoot)
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	for _, entry := range entries {
		// Alias symlinks (e.g. aarch64 -> arm64) point to other arch dirs.
		if entry.Mode()&os.ModeSymlink != 0 || !entry.IsDir() {
			continue
		}
		imgs, err := findImagesBelow(filepath.Join(multiarchRoot, entry.Name()), "")
		if err != nil {
			return nil, err
		}
		for _, img := range imgs {
			add(img)
		}
	}
	return result, nil
}

// findImagesBelow returns the images whose manifest directories
// (<registry>/<repo>:<tag>/manifest.json) are located under root. The
// directory skip, if non-empty, is not descended into.
func findImagesBelow(root, skip string) ([]*Image, error) {
	var result []*Image
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() || path == root {
			return nil
		}
		if skip != "" && path == skip {
			return filepath.SkipDir
		}
		if _, err := os.Stat(filepath.Join(path, "manifest.json")); err != nil {
			return nil
		}
		relPath, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		imageURL := fmt.Sprintf("https://%s", filepath.ToSlash(relPath))
		img, err := ParseImage(imageURL)
		if err != nil {
			l.LogE(err).WithFields(log.Fields{"path": path}).
				Warning("Failed to parse image from CVMFS metadata, skipping")
			return filepath.SkipDir
		}
		result = append(result, &img)
		// Manifest directories are never nested.
		return filepath.SkipDir
	})
	return result, err
}

// ImageCoveredByWishlist reports whether the given CVMFS image is covered by at
// least one wishlist entry. Registry and repository must match exactly; the tag
// must match exactly for fixed-tag entries, or as a glob pattern for
// wildcard-tagged entries.
func ImageCoveredByWishlist(img *Image, wishlist []Image) bool {
	for i := range wishlist {
		w := &wishlist[i]
		if w.Registry != img.Registry || w.Repository != img.Repository {
			continue
		}
		if w.TagWildcard {
			matched, err := filepath.Match(w.Tag, img.Tag)
			if err == nil && matched {
				return true
			}
			continue
		}
		if w.Tag == img.Tag {
			return true
		}
	}
	return false
}

// FindCVMFSImagesNotInWishlist returns all images converted into the CVMFS
// repository that are not covered by any entry of the given wishlist.
func FindCVMFSImagesNotInWishlist(CVMFSRepo string, wishlist []Image) ([]*Image, error) {
	all, err := FindAllCVMFSImages(CVMFSRepo)
	if err != nil {
		return nil, err
	}
	var result []*Image
	for _, img := range all {
		if ImageCoveredByWishlist(img, wishlist) {
			continue
		}
		result = append(result, img)
	}
	return result, nil
}

// DeleteImagesFromCVMFS deletes the given images from the CVMFS repository
// within a single transaction, so that pruning many images costs one publish.
// Errors on individual images are logged and do not stop the deletion of the
// remaining images; a partially removed image keeps some metadata and is
// picked up again on the next run. Returns the count of images deleted (or flagged in dry-run mode).
func DeleteImagesFromCVMFS(CVMFSRepo string, images []*Image, dryRun bool) (int, error) {
	type pending struct {
		img   *Image
		paths imageCVMFSPaths
	}
	var todo []pending
	for _, img := range images {
		paths := findImageCVMFSPaths(CVMFSRepo, img)
		if paths.empty() {
			l.Log().WithFields(log.Fields{"image": img.GetSimpleName()}).
				Info("Image not found in CVMFS, nothing to delete")
			continue
		}
		todo = append(todo, pending{img, paths})
	}

	if dryRun {
		for _, p := range todo {
			p.paths.printDryRun()
		}
		return len(todo), nil
	}
	if len(todo) == 0 {
		return 0, nil
	}

	deleted := 0
	err := cvmfs.WithinTransaction(CVMFSRepo, func() error {
		for _, p := range todo {
			if err := p.paths.remove(); err != nil {
				l.LogE(err).WithFields(log.Fields{"image": p.img.GetSimpleName()}).
					Warning("Error deleting image, continuing")
				continue
			}
			l.Log().WithFields(log.Fields{"image": p.img.GetSimpleName()}).
				Info("Deleted image from CVMFS")
			deleted++
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return deleted, nil
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

// PruneImages checks each image in the provided list and deletes from CVMFSRepo
// those that are no longer available on the registry. For wildcard-tagged images
// the registry tag list is fetched and any CVMFS entries not in that list are
// deleted. For fixed-tag images the tag is deleted if it no longer exists on the
// registry. Returns the count of images deleted (or flagged in dry-run mode).
func PruneImages(CVMFSRepo string, images []Image, dryRun bool) (int, error) {
	deleted := 0
	for i := range images {
		n, err := pruneOneImage(CVMFSRepo, &images[i], dryRun)
		if err != nil {
			l.LogE(err).WithFields(log.Fields{"image": images[i].GetSimpleName()}).
				Warning("Error pruning image, continuing")
		}
		deleted += n
	}
	return deleted, nil
}

// pruneOneImage determines which tags of img are no longer on the registry and
// deletes them from CVMFS. Returns the number of tags deleted.
func pruneOneImage(CVMFSRepo string, img *Image, dryRun bool) (int, error) {
	currentTags, err := currentTagsOnRegistry(img)
	if err != nil {
		return 0, fmt.Errorf("failed to query registry for %s: %w", img.GetSimpleName(), err)
	}

	cvmfsImages, err := FindCVMFSImagesMatchingPattern(CVMFSRepo, img.Registry, img.Repository, img.Tag)
	if err != nil {
		return 0, fmt.Errorf("failed to scan CVMFS for %s: %w", img.GetSimpleName(), err)
	}

	deleted := 0
	for _, cvmfsImg := range cvmfsImages {
		if currentTags[cvmfsImg.Tag] {
			continue
		}
		l.Log().WithFields(log.Fields{"image": cvmfsImg.GetSimpleName()}).
			Info("Image tag no longer available on registry, deleting from CVMFS")
		d, err := DeleteImageFromCVMFS(CVMFSRepo, cvmfsImg, dryRun)
		if err != nil {
			l.LogE(err).WithFields(log.Fields{"image": cvmfsImg.GetSimpleName()}).
				Warning("Error deleting image, continuing")
		}
		if d {
			deleted++
		}
	}
	return deleted, nil
}

// currentTagsOnRegistry returns the set of tags currently available on the
// registry for the given image. For wildcard tags the registry tag list is
// fetched and filtered; for fixed tags the single tag is included if it exists.
func currentTagsOnRegistry(img *Image) (map[string]bool, error) {
	result := make(map[string]bool)

	if img.TagWildcard {
		r1, _, err := img.ExpandWildcard()
		if err != nil {
			return nil, err
		}
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			for expanded := range r1 {
				result[expanded.Tag] = true
			}
		}()
		wg.Wait()
	} else {
		if img.ExistsOnRegistry() {
			result[img.Tag] = true
		}
	}

	return result, nil
}
