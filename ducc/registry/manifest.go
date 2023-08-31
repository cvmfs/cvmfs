package registry

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/cvmfs/ducc/db"
	"github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
)

type ManifestWithBytesAndDigest struct {
	Manifest       v1.Manifest
	ManifestBytes  []byte
	ManifestDigest digest.Digest
}

type ManifestListWithBytesAndDigest struct {
	ManifestList       v1.Index
	ManifestListBytes  []byte
	ManifestListDigest digest.Digest
}

var ErrNoManifestList = errors.New("manifest list not supported")

const TASK_FETCH_MANIFEST db.TaskType = "FETCH_MANIFEST"

func FetchManifestTask(tx *sql.Tx, image db.Image) (db.TaskPtr, error) {
	ownTx := false
	if tx == nil {
		var err error
		tx, err = db.GetTransaction()
		if err != nil {
			return db.NullTaskPtr(), err
		}
		defer tx.Rollback()
		ownTx = true
	}
	titleStr := fmt.Sprintf("Fetch manifest for %s", image.GetSimpleName())
	task, ptr, err := db.CreateTask(tx, TASK_FETCH_MANIFEST, titleStr)
	if err != nil {
		return db.NullTaskPtr(), err
	}

	if ownTx {
		if err := tx.Commit(); err != nil {
			return db.NullTaskPtr(), err
		}
	}

	go func() {
		if !task.WaitForStart() {
			return
		}
		manifest, _, gotManifestList, err := FetchAndParseManifestAndList(image)
		if err != nil {
			task.LogFatal(nil, fmt.Sprintf("error fetching manifest: %s", err))
			return
		}
		if gotManifestList {
			task.Log(nil, db.LOG_SEVERITY_DEBUG, "repository supplied manifest list")
		}

		task.SetArtifact(manifest)
		task.SetTaskCompleted(nil, db.TASK_RESULT_SUCCESS)
		task.Log(nil, db.LOG_SEVERITY_INFO, "Successfully fetched manifest")
	}()
	return ptr, nil
}

func FetchAndParseManifestAndList(image db.Image) (ManifestWithBytesAndDigest, ManifestListWithBytesAndDigest, bool, error) {
	var manifestListOut ManifestListWithBytesAndDigest
	var manifestOut ManifestWithBytesAndDigest

	var digestOrTag string
	if image.Digest != "" {
		digestOrTag = image.Digest.String()
	} else {
		digestOrTag = image.Tag
	}

	url := fmt.Sprintf("%s://%s/v2/%s/manifests/%s", image.RegistryScheme, image.RegistryHost, image.Repository, digestOrTag)
	req, _ := http.NewRequest("GET", url, nil)
	// We prefer the OCI formats, but fall back to the Docker formats
	req.Header.Add("Accept", "application/vnd.oci.image.index.v1+json")
	req.Header.Add("Accept", "application/vnd.docker.distribution.manifest.list.v2+json")
	req.Header.Add("Accept", "application/vnd.oci.image.manifest.v1+json")
	req.Header.Add("Accept", "application/vnd.docker.distribution.manifest.v2+json")

	// The registry handles authentication and backoff
	registry := GetOrCreateRegistry(ContainerRegistryIdentifier{Scheme: image.RegistryScheme, Hostname: image.RegistryHost})
	res, err := registry.PerformRequest(req, image.Repository)
	if err != nil {
		return manifestOut, manifestListOut, false, err
	}
	defer res.Body.Close()

	// Check the status code, give a more helpful error message if possible
	if res.StatusCode == http.StatusNotAcceptable {
		return manifestOut, manifestListOut, false, fmt.Errorf("registry does not support any of the accepted media types")
	} else if res.StatusCode != http.StatusOK {
		return manifestOut, manifestListOut, false, fmt.Errorf("unexpected status code: %d", res.StatusCode)
	}

	// Check the Content-Type header
	contentType := res.Header.Get("Content-Type")
	if contentType == "application/vnd.docker.distribution.manifest.list.v2+json" || contentType == "application/vnd.oci.image.index.v1+json" {
		// We got a manifest list
		var err error
		manifestListOut.ManifestListBytes, err = io.ReadAll(res.Body)
		if err != nil {
			return manifestOut, manifestListOut, false, err
		}
		if err := json.Unmarshal(manifestListOut.ManifestListBytes, &manifestListOut.ManifestList); err != nil {
			return manifestOut, manifestListOut, false, err
		}
		manifestListOut.ManifestListDigest = digest.FromBytes(manifestListOut.ManifestListBytes)

		var selectedManifestDigest digest.Digest
		// We need to find the correct manifest in the list
		if len(manifestListOut.ManifestList.Manifests) == 1 {
			// There is only one manifest, so we can just use that
			selectedManifestDigest = manifestListOut.ManifestList.Manifests[0].Digest
		} else {
			// We just pick the first one with "amd64"
			// TODO: Support multi-arch images
			for _, manifest := range manifestListOut.ManifestList.Manifests {
				if manifest.Platform.Architecture == "amd64" {
					selectedManifestDigest = manifest.Digest
					break
				}
			}
		}
		manifest, err := fetchAndParseManifestByBlobID(image, selectedManifestDigest)
		if err != nil {
			return manifestOut, manifestListOut, true, err
		}
		manifestOut = manifest
		return manifestOut, manifestListOut, true, nil
	}

	if contentType == "application/vnd.docker.distribution.manifest.v2+json" || contentType == "application/vnd.oci.image.manifest.v1+json" {
		// We got a manifest
		var err error
		manifestOut.ManifestBytes, err = io.ReadAll(res.Body)
		if err != nil {
			return manifestOut, manifestListOut, false, err
		}
		if err := json.Unmarshal(manifestOut.ManifestBytes, &manifestOut.Manifest); err != nil {
			return manifestOut, manifestListOut, false, err
		}
		manifestOut.ManifestDigest = digest.FromBytes(manifestOut.ManifestBytes)

		return manifestOut, manifestListOut, false, nil
	}

	return manifestOut, manifestListOut, false, fmt.Errorf("unexpected Content-Type: %s", contentType)
}

func fetchAndParseManifest(image db.Image) (ManifestWithBytesAndDigest, error) {
	var digestOrTag string
	if image.Digest != "" {
		digestOrTag = image.Digest.String()
	} else {
		digestOrTag = image.Tag
	}
	url := fmt.Sprintf("%s://%s/v2/%s/manifests/%s", image.RegistryScheme, image.RegistryHost, image.Repository, digestOrTag)
	registry := GetOrCreateRegistry(ContainerRegistryIdentifier{Scheme: image.RegistryScheme, Hostname: image.RegistryHost})
	return fetchAndParseManifestInternal(registry, image.Repository, url)
}

func fetchAndParseManifestByBlobID(image db.Image, blobID digest.Digest) (ManifestWithBytesAndDigest, error) {
	url := fmt.Sprintf("%s://%s/v2/%s/manifests/%s", image.RegistryScheme, image.RegistryHost, image.Repository, blobID)
	registry := GetOrCreateRegistry(ContainerRegistryIdentifier{Scheme: image.RegistryScheme, Hostname: image.RegistryHost})
	return fetchAndParseManifestInternal(registry, image.Repository, url)
}

func fetchAndParseManifestInternal(registry *ContainerRegistry, repository string, url string) (ManifestWithBytesAndDigest, error) {
	var out ManifestWithBytesAndDigest
	req, _ := http.NewRequest("GET", url, nil)
	// We prefer the OCI manifest, but fall back to the Docker manifest
	// if the OCI manifest is not available.
	req.Header.Add("Accept", "application/vnd.oci.image.manifest.v1+json")
	req.Header.Add("Accept", "application/vnd.docker.distribution.manifest.v2+json")
	// Temporary add in manifest list support

	// The registry handles authentication and backoff
	res, err := registry.PerformRequest(req, repository)
	if err != nil {
		return out, err
	}
	defer res.Body.Close()

	// Check the status code, give a more helpful error message if possible
	if res.StatusCode == http.StatusNotAcceptable {
		return out, fmt.Errorf("registry does not support any of the accepted media types")
	} else if res.StatusCode != http.StatusOK {
		return out, fmt.Errorf("unexpected status code: %d", res.StatusCode)
	}

	// Check the Content-Type header
	contentType := res.Header.Get("Content-Type")
	if contentType != "application/vnd.docker.distribution.manifest.v2+json" && contentType != "application/vnd.oci.image.manifest.v1+json" {
		return out, fmt.Errorf("unexpected Content-Type: %s", contentType)
	}

	// Read and unmarshal the manifest to the out struct
	out.ManifestBytes, err = io.ReadAll(res.Body)
	if err != nil {
		return out, err
	}
	if err := json.Unmarshal(out.ManifestBytes, &out.Manifest); err != nil {
		return out, err
	}
	out.ManifestDigest = digest.SHA256.FromBytes(out.ManifestBytes)

	return out, nil
}

func fetchAndParseManifestList(image db.Image) (ManifestListWithBytesAndDigest, error) {
	var out ManifestListWithBytesAndDigest

	var digestOrTag string
	if image.Digest != "" {
		digestOrTag = image.Digest.String()
	} else {
		digestOrTag = image.Tag
	}

	url := fmt.Sprintf("%s://%s/v2/%s/manifests/%s", image.RegistryScheme, image.RegistryHost, image.Repository, digestOrTag)
	req, _ := http.NewRequest("GET", url, nil)

	// We prefer the OCI index, but fall back to the Docker manifest list
	// if the OCI index is not available.
	req.Header.Add("Accept", "application/vnd.oci.image.index.v1+json")
	req.Header.Add("Accept", "application/vnd.docker.distribution.manifest.list.v2+json")

	// The registry handles authentication and backoff
	registry := GetOrCreateRegistry(ContainerRegistryIdentifier{Scheme: image.RegistryScheme, Hostname: image.RegistryHost})
	res, err := registry.PerformRequest(req, image.Repository)
	if err != nil {
		return out, err
	}
	defer res.Body.Close()

	// Check the status code, give a more helpful error message if possible
	if res.StatusCode == http.StatusNotAcceptable {
		return out, fmt.Errorf("registry does not support any of the accepted media types")
	} else if res.StatusCode != http.StatusOK {
		return out, fmt.Errorf("unexpected status code: %d", res.StatusCode)
	}

	// Check the Content-Type header
	contentType := res.Header.Get("Content-Type")
	if contentType != "application/vnd.docker.distribution.manifest.list.v2+json" && contentType != "application/vnd.oci.image.index.v1+json" {
		return out, fmt.Errorf("unexpected Content-Type: %s", contentType)
	}

	// Read and unmarshal the manifest to the out struct
	out.ManifestListBytes, err = io.ReadAll(res.Body)
	if err != nil {
		return out, err
	}
	if err := json.Unmarshal(out.ManifestListBytes, &out.ManifestList); err != nil {
		return out, err
	}
	out.ManifestListDigest = digest.SHA256.FromBytes(out.ManifestListBytes)

	return out, nil
}
