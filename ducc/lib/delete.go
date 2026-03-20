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

// DeleteImageFromCVMFS deletes the user-facing symlink and manifest directory
// for the given image from the CVMFS repository, making it eligible for
// garbage collection on the next run. Returns true if any paths existed and
// were (or would be) deleted.
func DeleteImageFromCVMFS(CVMFSRepo string, img *Image, dryRun bool) (bool, error) {
	symlinkPath := filepath.Join("/", "cvmfs", CVMFSRepo, img.GetPublicSymlinkPath())
	manifestDir := filepath.Join("/", "cvmfs", CVMFSRepo, ".metadata", img.GetSimpleName())

	_, symlinkErr := os.Lstat(symlinkPath)
	_, manifestErr := os.Stat(manifestDir)

	symlinkExists := symlinkErr == nil
	manifestExists := manifestErr == nil

	if !symlinkExists && !manifestExists {
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
		return nil
	})
	if err != nil {
		return true, err
	}

	l.Log().WithFields(log.Fields{"image": img.GetSimpleName()}).
		Info("Deleted image from CVMFS")
	return true, nil
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
