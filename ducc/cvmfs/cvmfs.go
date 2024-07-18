package cvmfs

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"golang.org/x/sys/unix"

	"github.com/pkg/xattr"

	"github.com/cvmfs/ducc/config"
)

type TransactionOption interface {
	ToString() string
}

type TemplateTransaction struct {
	Source      string
	Destination string
}

func NewTemplateTransaction(source, destination string) TemplateTransaction {
	return TemplateTransaction{source, destination}
}

func (t TemplateTransaction) ToString() string {
	return fmt.Sprintf("-T %s=%s", t.Source, t.Destination)
}

// from /cvmfs/$REPO/foo/bar -> foo/bar
func TrimCVMFSRepoPrefix(path string) string {
	return strings.Join(strings.Split(path, string(os.PathSeparator))[3:], string(os.PathSeparator))
}

func IsWhiteout(path string) bool {
	base := filepath.Base(path)
	if len(base) <= 3 {
		return false
	}
	return base[0:4] == ".wh."
}

func OpenTransactionNew(cvmfsRepo string, opts ...TransactionOption) (success bool, stdout string, stderr string, err error) {
	allOpts := []string{"transaction"}
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
		err = os.MkdirAll(dir, config.DirPermision)
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

func Ingest(CVMFSRepo string, input io.ReadCloser, options ...string) error {
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

func MakeWhiteoutFile(path string) error {
	dev := unix.Mkdev(0, 0)
	mode := os.FileMode(int64(unix.S_IFCHR) | 0000)
	return unix.Mknod(path, uint32(mode), int(dev))
}

func MakeOpaqueDir(path string) error {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		if err := os.MkdirAll(path, config.DirPermision); err != nil {
			return err
		}
	}
	return xattr.Set(path, "trusted.overlay.opaque", []byte("y"))
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

func RepositoryExists(CVMFSRepo string) (bool, error) {
	success, stdOut, stdErr, err := runCommand("cvmfs_server", "list")

	if err != nil {
		return false, fmt.Errorf("could not list repositories: %v", err)
	}
	if !success {
		return false, fmt.Errorf("could not list repositories: %s", stdErr)
	}

	strings.Split(stdOut, "\n")
	for _, line := range strings.Split(stdOut, "\n") {
		if strings.HasPrefix(line, CVMFSRepo) {
			return true, nil
		}
	}
	return false, nil
}

// TODO: Implement remove schedule, garbage collection

/*
func RemoveScheduleLocation(CVMFSRepo string) string {
	return filepath.Join("/", "cvmfs", CVMFSRepo, ".metadata", "remove-schedule.json")
}

func AddManifestToRemoveScheduler(CVMFSRepo string, manifest da.Manifest) error {
	schedulePath := RemoveScheduleLocation(CVMFSRepo)
	llog := func(l *log.Entry) *log.Entry {
		return l.WithFields(log.Fields{
			"action": "add manifest to remove schedule",
			"file":   schedulePath})
	}
	var schedule []da.Manifest

	// if the file exist, load from it
	if _, err := os.Stat(schedulePath); !os.IsNotExist(err) {

		scheduleFileRO, err := os.OpenFile(schedulePath, os.O_RDONLY, config.FilePermision)
		if err != nil {
			llog(l.LogE(err)).Error("Impossible to open the schedule file")
			return err
		}

		scheduleBytes, err := ioutil.ReadAll(scheduleFileRO)
		if err != nil {
			llog(l.LogE(err)).Error("Impossible to read the schedule file")
			return err
		}

		err = scheduleFileRO.Close()
		if err != nil {
			llog(l.LogE(err)).Error("Impossible to close the schedule file")
			return err
		}

		err = json.Unmarshal(scheduleBytes, &schedule)
		if err != nil {
			llog(l.LogE(err)).Error("Impossible to unmarshal the schedule file")
			return err
		}
	}

	schedule = func() []da.Manifest {
		for _, m := range schedule {
			if m.Config.Digest == manifest.Config.Digest {
				return schedule
			}
		}
		schedule = append(schedule, manifest)
		return schedule
	}()

	err := WithinTransactionOld(CVMFSRepo, func() error {
		if _, err := os.Stat(schedulePath); os.IsNotExist(err) {
			err = os.MkdirAll(filepath.Dir(schedulePath), config.DirPermision)
			if err != nil {
				llog(l.LogE(err)).Error("Error in creating the directory where save the schedule")
			}
		}

		bytes, err := json.Marshal(schedule)
		if err != nil {
			llog(l.LogE(err)).Error("Error in marshaling the new schedule")
		} else {

			err = ioutil.WriteFile(schedulePath, bytes, config.FilePermision)
			if err != nil {
				llog(l.LogE(err)).Error("Error in writing the new schedule")
			} else {
				llog(l.Log()).Info("Wrote new remove schedule")
			}
		}
		return nil
	})

	return err
}

func RemoveSingularityImageFromManifest(CVMFSRepo string, manifest da.Manifest) error {
	dir := filepath.Join("/", "cvmfs", CVMFSRepo, manifest.GetSingularityPath())
	llog := func(l *log.Entry) *log.Entry {
		return l.WithFields(log.Fields{
			"action": "removing singularity directory", "directory": dir})
	}
	err := RemoveDirectory(CVMFSRepo, manifest.GetSingularityPath())
	if err != nil {
		llog(l.LogE(err)).Error("Error in removing singularity direcotry")
		return err
	}
	return nil
}
*/
