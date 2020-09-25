package cvmfs

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/cvmfs/ducc/lib"
)

type FSOperation interface {
	// this function is called to modify the file system, it should modify only the paths returned by `Paths`
	// if this method returns an error, the whole transaction is aborted
	Execute() error
	// returns the paths that the operation modify inside the repository, the paths must include the `/cvmfs/$reponame` prefix
	Paths() []string
	// called after the Execute function and after committing the transaction against the repository, it is called in a different goroutine
	// the parameter is true if the transaction was successfully commited, false otherwise
	Commit(transactionOk bool)
	// must return true if the operation need to run outside one transaction, for  instance `ingest` operations
	RunOutsideTransaction() bool
}

func failedTransactionError() error { return fmt.Errorf("failed transaction") }

type CreateDirectory struct {
	path string
}

func NewCreateDirectory(path string) *CreateDirectory {
	return &CreateDirectory{path}
}

func (c *CreateDirectory) Execute() error {
	return os.MkdirAll(c.path, 0755)
}

func (c *CreateDirectory) Paths() []string {
	return []string{c.path}
}

func (c *CreateDirectory) Commit(txtOk bool) {}

func (c *CreateDirectory) RunOutsideTransaction() bool { return false }

type CopyFile struct {
	src  string
	dest string
	err  chan error
}

func NewCopyFile(src, dest string) (*CopyFile, error) {
	if _, err := os.Stat(src); err != nil {
		return &CopyFile{}, err
	}
	return &CopyFile{src, dest, make(chan error, 2)}, nil
}

func (op *CopyFile) Paths() []string {
	return []string{op.dest}
}

func (op *CopyFile) Execute() error {
	err := func() error {
		from, err := os.Open(op.src)
		if err != nil {
			return err
		}
		os.MkdirAll(filepath.Dir(op.dest), 0755)
		os.Remove(op.dest)

		defer from.Close()
		to, err := os.OpenFile(op.dest, os.O_WRONLY|os.O_CREATE, 0644)
		if err != nil {
			return err
		}
		defer to.Close()
		_, err = io.Copy(to, from)
		return err
	}()
	op.err <- err
	return nil
}

func (op *CopyFile) Commit(transactionOk bool) {
	defer close(op.err)
	if transactionOk == false {
		op.err <- failedTransactionError()
	}
}

func (op *CopyFile) RunOutsideTransaction() bool { return false }
func (op *CopyFile) errorsChannel() chan error   { return op.err }

func (op *CopyFile) SuccesfullyCompleted() bool {
	for err := range op.errorsChannel() {
		if err != nil {
			return false
		}
	}
	return true
}

type IngestTar struct {
	tar  io.ReadCloser
	path string
	err  chan error
}

func NewIngestTar(tar io.ReadCloser, path string) *IngestTar {
	return &IngestTar{tar, path, make(chan error, 2)}
}

func (op *IngestTar) Execute() error {
	err := func() error {
		tokens := strings.Split(op.path, string(os.PathSeparator))
		repo := tokens[2]
		path := strings.Join(tokens[3:], string(os.PathSeparator))
		return lib.ExecCommand("cvmfs_server", "ingest", "--catalog", "-t", "-", "-b", path, repo).StdIn(op.tar).Start()
	}()
	op.err <- err
	return nil
}

func (op *IngestTar) Paths() []string {
	return []string{op.path}
}

func (op *IngestTar) Commit(transactionOk bool) {
	defer close(op.err)
	if transactionOk == false {
		op.err <- failedTransactionError()
	}
}

func (op *IngestTar) RunOutsideTransaction() bool { return true }

type DeletePathIngest struct {
	path string
	err  chan error
}

func NewDeletePathIngest(path string) *DeletePathIngest {
	return &DeletePathIngest{path, make(chan error, 2)}
}

func (op *DeletePathIngest) Execute() error {
	err := func() error {
		tokens := strings.Split(op.path, string(os.PathSeparator))
		repo := tokens[2]
		path := strings.Join(tokens[3:], string(os.PathSeparator))
		return lib.ExecCommand("cvmfs_server", "ingest", "--delete", path, repo).Start()
	}()
	op.err <- err
	return nil
}
func (op *DeletePathIngest) Paths() []string {
	return []string{op.path}
}
func (op *DeletePathIngest) Commit(transactionOk bool) {
	defer close(op.err)
	if transactionOk == false {
		op.err <- failedTransactionError()
	}
}
func (op *DeletePathIngest) RunOutsideTransaction() bool { return true }

type Delete struct {
	paths []string
	err   chan error
}

func NewDelete(paths ...string) *Delete {
	toDelete := []string{}
	for _, p := range paths {
		toDelete = append(toDelete, p)
	}
	return &Delete{toDelete, make(chan error, 2)}
}

func (op *Delete) Execute() error {
	err := func() error {
		for _, path := range op.paths {
			err := os.RemoveAll(path)
			if err != nil {
				return err
			}
		}
		return nil
	}()
	op.err <- err
	return nil
}

func (op *Delete) Paths() []string {
	return op.paths
}

func (op *Delete) Commit(transactionOk bool) {
	defer close(op.err)
	if transactionOk == false {
		op.err <- failedTransactionError()
	}
}

func (op *Delete) RunOutsideTransaction() bool { return false }

type CreateSymlink struct {
	// name -> target
	name   string
	target string
	err    chan error
}

func NewCreateSymlink(name, target string) *CreateSymlink {
	return &CreateSymlink{name, target, make(chan error, 2)}
}

func (op *CreateSymlink) Execute() error {
	err := func() error {
		// we remove the path that exists now
		os.RemoveAll(op.name)

		// we create the directory structure that point to the new link
		linkDir := filepath.Dir(op.name)
		err := os.MkdirAll(linkDir, 0755)
		if err != nil {
			return err
		}

		// we don't use directly the name since the name can contains
		// symlinks itself.
		// in this way we evaluate along with all the symlinks outside
		name, err := filepath.EvalSymlinks(linkDir)
		if err != nil {
			name = linkDir
		}
		name = filepath.Join(name, filepath.Base(op.name))

		relativePath, err := filepath.Rel(name, op.target)
		if err != nil {
			return err
		}

		linkChunks := strings.Split(relativePath, string(os.PathSeparator))
		relativeTarget := filepath.Join(linkChunks[1:]...)

		if err != nil {
			return err
		}
		return os.Symlink(relativeTarget, op.name)
	}()
	op.err <- err
	return nil
}

func (op *CreateSymlink) Paths() []string { return []string{op.name} }

func (op *CreateSymlink) Commit(transactionOk bool) {
	defer close(op.err)
	if transactionOk == false {
		op.err <- failedTransactionError()
	}
}

func (op *CreateSymlink) RunOutsideTransaction() bool { return false }

type CopyDirectory struct{}
