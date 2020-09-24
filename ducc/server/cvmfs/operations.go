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

func (c *CreateDirectory) RunInPrivateTransaction() bool { return false }
func (c *CreateDirectory) RunOutsideTransaction() bool   { return false }

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
		op.err <- fmt.Errorf("failed transaction")
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
		op.err <- fmt.Errorf("failed transaction")
	}
}

func (op *IngestTar) RunOutsideTransaction() bool { return true }
