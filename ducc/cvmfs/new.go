package cvmfs

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"

	"os/exec"

	"github.com/cvmfs/ducc/constants"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
)

func OpenTransactionNew(cvmfsRepo string, opts ...TransactionOption) (success bool, stdout string, stderr string, err error) {
	allOpts := append([]string{"transaction"})
	for _, opt := range opts {
		allOpts = append(allOpts, opt.ToString())
	}
	allOpts = append(allOpts, cvmfsRepo)
	return runCommand("cvmfs_server", allOpts...)
}

func AbortTransactionNew(cvmfsRepo string) (err error) {
	// We don't care about the output of the command
	return exec.Command("cvmfs_server", "abort", "-f", cvmfsRepo).Start()
}

func PublishTransactionNew(cvmfsRepo string) (success bool, stdout string, stderr string, err error) {
	return runCommand("cvmfs_server", "publish", cvmfsRepo)
}

func WithinTransactionNew(CVMFSRepo string, f func() error, opts ...TransactionOption) (success bool, err error) {
	succcess, _, stderr, err := OpenTransactionNew(CVMFSRepo, opts...)
	if err != nil {
		return false, fmt.Errorf("could not open transaction: %v", err)
	}
	if !succcess {
		return false, fmt.Errorf("could not open transaction: %s", stderr)
	}
	err = f()
	if err != nil {
		err := AbortTransactionNew(CVMFSRepo)
		if err != nil {
			return false, fmt.Errorf("could not abort transaction: %v", err)
		}
		return false, nil
	}

	succcess, _, stderr, err = PublishTransactionNew(CVMFSRepo)
	if err != nil {
		return false, fmt.Errorf("could not publish transaction: %v", err)
	}
	if !succcess {
		return false, fmt.Errorf("could not publish transaction: %s", stderr)
	}
	return true, nil
}
func CreateCatalogNew(dir string) (changed bool, err error) {
	catalogPath := filepath.Join(dir, ".cvmfscatalog")
	changed = false
	// Check that the directory exists
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		err = os.MkdirAll(dir, constants.DirPermision)
		if err != nil {
			return false, fmt.Errorf("could not create directory: %v", err)
		}
		changed = true
	}
	if _, err := os.Stat(catalogPath); os.IsNotExist(err) {
		file, err := os.Create(catalogPath)
		if err != nil {
			return changed, fmt.Errorf("could not create catalog file: %v", err)
		}
		changed = true
		file.Close()
	}
	return changed, nil
}

func runCommand(name string, args ...string) (success bool, stdout string, stderr string, err error) {
	cmd := exec.Command(name, args...)

	var stdoutBuf, stderrBuf bytes.Buffer
	cmd.Stdout = &stdoutBuf
	cmd.Stderr = &stderrBuf

	err = cmd.Run()
	stdoutContent := stdoutBuf.String()
	stderrContent := stderrBuf.String()

	if err != nil {
		if _, ok := err.(*exec.ExitError); ok {
			// Command has returned a non-zero exit code.
			return false, stdoutContent, stderrContent, nil
		} else {
			// Something else went wrong, e.g., the command was not found.
			return false, stdoutContent, stderrContent, fmt.Errorf("command execution failed: %v", err)
		}
	}
	return true, stdoutContent, stderrContent, nil
}

func IngestNew(CVMFSRepo string, input io.ReadCloser, options ...string) error {
	opts := []string{"ingest"}
	opts = append(opts, options...)
	opts = append(opts, CVMFSRepo)
	success, _, stdErr, err := runCommand("cvmfs_server", opts...)
	if err != nil {
		return fmt.Errorf("ingest failed. Error running command: %v", err)
	}
	if !success {
		return fmt.Errorf("ingest failed: %s", stdErr)
	}
	return nil
}

func IngestDeleteNew(CVMFSRepo string, path string) error {
	success, _, stdErr, err := runCommand("cvmfs_server", "ingest", "--delete", path, CVMFSRepo)
	if err != nil {
		return fmt.Errorf("ingestDelete failed. Error running command: %v", err)
	}
	if !success {
		return fmt.Errorf("ingestDelete failed: %s", stdErr)
	}
	return nil
}

type Backlink struct {
	Origin []string `json:"origin"`
}

func GetBacklinkPath(CVMFSRepo, layerDigest string) string {
	return filepath.Join(LayerMetadataPath(CVMFSRepo, layerDigest), "origin.json")
}

func GetBacklinkFromLayerNew(CVMFSRepo, layerDigest string) (backlink Backlink, err error) {
	backlinkPath := GetBacklinkPath(CVMFSRepo, layerDigest)

	if _, err := os.Stat(backlinkPath); os.IsNotExist(err) {
		return Backlink{Origin: []string{}}, nil
	}

	backlinkFile, err := os.Open(backlinkPath)
	if err != nil {
		return backlink, fmt.Errorf("error in opening the backlink file: %v", err)
	}

	byteBackLink, err := ioutil.ReadAll(backlinkFile)
	if err != nil {
		return backlink, fmt.Errorf("error in reading the bytes from the origin file: %v", err)
	}

	err = backlinkFile.Close()
	if err != nil {
		return backlink, fmt.Errorf("error in closing the file after reading: %v", err)
	}

	err = json.Unmarshal(byteBackLink, &backlink)
	if err != nil {
		return backlink, fmt.Errorf("error in unmarshaling the files: %v", err)
	}
	return backlink, nil
}

// Need to be in a transaction when calling this function
func CreateLayersBacklinkNew(CVMFSRepo string, manifest v1.Manifest, imageName string) error {

	backlinks := make(map[string][]byte)

	for _, layer := range manifest.Layers {
		configDigest := manifest.Config.Digest.String()

		backlink, err := GetBacklinkFromLayerNew(CVMFSRepo, layer.Digest.Encoded())
		if err != nil {
			//TODO: Log "Error in obtaining the backlink from a layer digest, skipping..."
		}
		backlink.Origin = append(backlink.Origin, configDigest)

		backlinkBytesMarshal, err := json.Marshal(backlink)
		if err != nil {
			// TODO: Log "error in marshaling backlinks"
			continue
		}

		backlinkPath := GetBacklinkPath(CVMFSRepo, layer.Digest.Encoded())
		backlinks[backlinkPath] = backlinkBytesMarshal
	}

	for path, fileContent := range backlinks {
		// the path may not be there, check,
		// and if it doesn't exists create it
		dir := filepath.Dir(path)
		if _, err := os.Stat(dir); os.IsNotExist(err) {
			err = os.MkdirAll(dir, constants.DirPermision)
			if err != nil {
				// TODO: Log "Error in creating the directory for the backlinks file, skipping..."
				continue
			}
		}
		err := os.WriteFile(path, fileContent, constants.FilePermision)
		if err != nil {
			// TODO: Log "Error in writing the backlinks file"
			continue
		}
	}
	return nil
}
