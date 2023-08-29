package cvmfs

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"os/exec"

	"github.com/cvmfs/ducc/constants"
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

func RemoveDirectoryNew(CVMFSRepo string, dirPath ...string) error {
	path := []string{"/cvmfs", CVMFSRepo}
	for _, p := range dirPath {
		path = append(path, p)
	}
	directory := filepath.Join(path...)
	stat, err := os.Stat(directory)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	if !stat.Mode().IsDir() {
		err = fmt.Errorf("Trying to remove something different from a directory")
		return err
	}

	dirsSplitted := strings.Split(directory, string(os.PathSeparator))
	if len(dirsSplitted) <= 3 || dirsSplitted[1] != "cvmfs" {
		err := fmt.Errorf("directory not in the CVMFS repo")
		return err
	}
	_, err = WithinTransactionNew(CVMFSRepo, func() error {
		err := os.RemoveAll(directory)
		if err != nil {
		}
		return err
	})

	return err
}
