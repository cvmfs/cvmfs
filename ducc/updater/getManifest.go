package updater

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/cvmfs/ducc/db"
	"github.com/cvmfs/ducc/registry"
	"github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
)

func FetchManifestAndSaveDigest(image db.Image) (v1.Manifest, digest.Digest, error) {
	manifest, manifestDigest, err := fetchManifest(image)
	if err != nil {
		return v1.Manifest{}, "", err
	}

	image.Digest = manifestDigest
	err = db.UpdateManifestDigestByImageID(nil, image.ID, manifestDigest)
	if err != nil {
		return v1.Manifest{}, "", err
	}

	return manifest, manifestDigest, nil
}

func fetchManifest(image db.Image) (v1.Manifest, digest.Digest, error) {
	var digestOrTag string
	if image.Digest != "" {
		digestOrTag = image.Digest.String()
	} else {
		digestOrTag = image.Tag
	}

	url := fmt.Sprintf("%s://%s/v2/%s/manifests/%s", image.RegistryScheme, image.RegistryHost, image.Repository, digestOrTag)
	req, _ := http.NewRequest("GET", url, nil)
	// TODO: Support manifest lists and potentially other media types
	req.Header.Add("Accept", "application/vnd.docker.distribution.manifest.v2+json")
	req.Header.Add("Accept", "application/vnd.oci.image.manifest.v1+json")

	// The registry handles authentication and backoff
	registry := registry.GetOrCreateRegistry(registry.ContainerRegistryIdentifier{Scheme: image.RegistryScheme, Hostname: image.RegistryHost})
	res, err := registry.PerformRequest(req)
	if err != nil {
		return v1.Manifest{}, "", err
	}
	defer res.Body.Close()

	bytes, err := io.ReadAll(res.Body)
	if err != nil {
		return v1.Manifest{}, "", err
	}
	manifest, fileDigest, err := parseManifestFromBytes(bytes)
	if err != nil {
		return v1.Manifest{}, "", err
	}

	return manifest, fileDigest, nil
}

func parseManifestFromBytes(bytes []byte) (v1.Manifest, digest.Digest, error) {
	var out v1.Manifest
	err := json.Unmarshal(bytes, &out)
	if err != nil {
		return v1.Manifest{}, "", fmt.Errorf("error in decoding the manifest from the server: %s", err)
	}
	manifestDigest := digest.SHA256.FromBytes(bytes)

	return out, manifestDigest, nil
}
