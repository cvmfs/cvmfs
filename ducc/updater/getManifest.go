package updater

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/cvmfs/ducc/db"
	"github.com/cvmfs/ducc/registry"
	"github.com/google/uuid"
	"github.com/opencontainers/go-digest"
)

func GetAndStoreManifest(image db.Image) error {
	if (image.ID == uuid.TaskID{}) {
		return fmt.Errorf("image is missing ID")
	}

	manifest, err := fetchManifest(image)
	if err != nil {
		return err
	}

	tx, err := db.GetTransaction()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Ensure that the image still exists. Also makes sure we have the correct ID.
	image, err = db.GetImageByValue(tx, image)
	if err == sql.ErrNoRows {
		return errors.New("image no longer exists")
	} else if err != nil {
		return err
	}

	// Update the manifest for the image
	err = db.CreateManifestForImageID(tx, image.ID, manifest)
	if err != nil {
		return err
	}

	err = tx.Commit()
	if err != nil {
		return err
	}

	return nil
}

func fetchManifest(image db.Image) (db.Manifest, error) {
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

	// The registry handles authentication and backoff
	registry := registry.GetOrCreateRegistry(registry.ContainerRegistryIdentifier{Scheme: image.RegistryScheme, Hostname: image.RegistryHost})
	res, err := registry.PerformRequest(req)
	if err != nil {
		return db.Manifest{}, err
	}
	defer res.Body.Close()

	bytes, err := io.ReadAll(res.Body)
	if err != nil {
		return db.Manifest{}, err
	}
	manifest, err := parseManifestFromBytes(bytes)
	if err != nil {
		return db.Manifest{}, err
	}

	return manifest, nil
}

func parseManifestFromBytes(bytes []byte) (db.Manifest, error) {
	var out db.Manifest

	err := json.Unmarshal(bytes, &out)
	if err != nil {
		return db.Manifest{}, fmt.Errorf("error in decoding the manifest from the server: %s", err)
	}
	out.FileDigest = digest.SHA256.FromBytes(bytes)

	return out, nil
}
