package registry

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/cvmfs/ducc/db"
	"github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
)

type ConfigWithBytesAndDigest struct {
	Config       v1.Image
	ConfigBytes  []byte
	ConfigDigest digest.Digest
}

func fetchAndParseConfig(image db.Image, configDigest digest.Digest) (ConfigWithBytesAndDigest, error) {
	var out ConfigWithBytesAndDigest

	url := fmt.Sprintf("%s://%s/v2/%s/blobs/%s", image.RegistryScheme, image.RegistryHost, image.Repository, configDigest.String())
	req, _ := http.NewRequest("GET", url, nil)

	// We prefer the OCI config, but fall back to the Docker config
	// if the OCI config is not available.
	req.Header.Add("Accept", "application/vnd.oci.image.config.v1+json")
	req.Header.Add("Accept", "application/vnd.docker.container.image.v1+json")

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
	if contentType != "application/vnd.docker.container.image.v1+json" && contentType != "application/vnd.oci.image.config.v1+json" {
		// Apparently some registries return the config with the wrong Content-Type
		//return out, fmt.Errorf("unexpected Content-Type: %s", contentType)

	}

	// Read and unmarshal the manifest to the out struct
	out.ConfigBytes, err = io.ReadAll(res.Body)
	if err != nil {
		return out, err
	}
	if err := json.Unmarshal(out.ConfigBytes, &out.Config); err != nil {
		return out, err
	}
	out.ConfigDigest = digest.SHA256.FromBytes(out.ConfigBytes)

	return out, nil
}

func FetchAndParseConfigTask(image db.Image, configDigest digest.Digest) (db.TaskPtr, error) {
	titleStr := fmt.Sprintf("Fetch and parse config for %s", image.GetSimpleName())
	task, ptr, err := db.CreateTask(nil, db.TASK_FETCH_OCI_CONFIG, titleStr)
	if err != nil {
		return db.TaskPtr{}, err
	}
	go func() {
		if !task.WaitForStart() {
			return
		}
		task.Log(nil, db.LOG_SEVERITY_DEBUG, fmt.Sprintf("Fetching and parsing config %s", configDigest.String()))
		config, err := fetchAndParseConfig(image, configDigest)
		if err != nil {
			task.LogFatal(nil, fmt.Sprintf("Failed to fetch and parse config %s: %s", configDigest.String(), err.Error()))
			return
		}
		task.Log(nil, db.LOG_SEVERITY_DEBUG, fmt.Sprintf("Finished fetching and parsing config %s", configDigest.String()))
		task.SetArtifact(config)
		_ = task.SetTaskCompleted(nil, db.TASK_RESULT_SUCCESS)
	}()

	return ptr, nil
}
