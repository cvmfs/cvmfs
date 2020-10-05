package cvmfs

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/cvmfs/ducc/lib"
	"github.com/cvmfs/ducc/server/cvmfs/applyDirectory"
	"github.com/cvmfs/ducc/server/cvmfs/singularity"
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

type fsOperation interface {
	Execute() error
	Paths() []string
}

type InternalFsOperation struct {
	fsOperation
	err chan error
}

func newInternalFsOperation(op fsOperation, chansize int) *InternalFsOperation {
	return &InternalFsOperation{op, make(chan error, chansize)}
}

func (op *InternalFsOperation) Execute() error {
	err := op.fsOperation.Execute()
	op.err <- err
	return nil
}

func (op *InternalFsOperation) FirstError() error {
	for e := range op.err {
		if e != nil {
			return e
		}
	}
	return nil
}

func (op *InternalFsOperation) Commit(transactionOk bool) {
	defer close(op.err)
	if transactionOk == false {
		op.err <- failedTransactionError()
	}
}

func (op *InternalFsOperation) RunOutsideTransaction() bool { return false }

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
	tar        io.ReadCloser
	path       string
	err        chan error
	firstError error
}

func NewIngestTar(tar io.ReadCloser, path string) *IngestTar {
	return &IngestTar{tar, path, make(chan error, 2), nil}
}

func (op *IngestTar) Execute() error {
	tokens := strings.Split(op.path, string(os.PathSeparator))
	repo := tokens[2]
	path := strings.Join(tokens[3:], string(os.PathSeparator))
	err := lib.ExecCommand("cvmfs_server", "ingest", "--catalog", "-t", "-", "-b", path, repo).StdIn(op.tar).Start()
	op.err <- err
	return err
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

func (op *IngestTar) FirstError() error {
	for e := range op.err {
		if e != nil {
			op.firstError = e
		}
	}
	return op.firstError
}

type IngestTarIfNotExists struct {
	*IngestTar
}

func NewIngestTarIfNotExists(tar io.ReadCloser, path string) *IngestTarIfNotExists {
	inner := NewIngestTar(tar, path)
	return &IngestTarIfNotExists{inner}
}

func (op *IngestTarIfNotExists) Execute() error {
	stat, err := os.Open(op.IngestTar.path)
	if err == nil { // the directory exists
		files, err := stat.Readdir(1)
		if err == nil && len(files) > 0 {
			// there are files inside the directory
			return nil
		}
	}
	return op.IngestTar.Execute()
}

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

func (op *CreateSymlink) FirstError() error {
	for e := range op.err {
		if e != nil {
			return e
		}
	}
	return nil
}

type AddCVMFSCatalog struct {
	directory string
	err       chan error
}

func NewAddCVMFSCatalog(path string) *AddCVMFSCatalog {
	return &AddCVMFSCatalog{path, make(chan error, 2)}
}

func (op *AddCVMFSCatalog) Paths() []string { return []string{op.directory} }

func (op *AddCVMFSCatalog) Execute() error {
	err := func() error {
		path := filepath.Join(op.directory, ".cvmfscatalog")
		if _, err := os.Stat(path); err == nil {
			return nil
		}
		err := os.MkdirAll(op.directory, 0755)
		if err != nil {
			return err
		}
		f, err := os.Create(path)
		defer f.Close()
		return err
	}()
	op.err <- err
	return nil
}

func (op *AddCVMFSCatalog) Commit(transactionOk bool) {
	defer close(op.err)
	if transactionOk == false {
		op.err <- failedTransactionError()
	}
}

func (op *AddCVMFSCatalog) RunOutsideTransaction() bool { return false }

type ApplyDirectories struct {
	target  string
	toApply []string
	err     chan error
}

func NewApplyDirectories(target string, directories ...string) *ApplyDirectories {
	toApply := []string{}
	for _, dir := range directories {
		toApply = append(toApply, dir)
	}
	return &ApplyDirectories{target, toApply, make(chan error, 2)}
}

func (op *ApplyDirectories) Paths() []string { return []string{op.target} }

func (op *ApplyDirectories) Execute() error {
	err := func() error {
		if err := os.MkdirAll(op.target, 0755); err != nil {
			return err
		}
		for _, apply := range op.toApply {
			if err := applyDirectory.ApplyDirectory(op.target, apply); err != nil {
				os.RemoveAll(op.target)
				return err
			}
		}
		return nil
	}()
	op.err <- err
	return nil
}

func (op *ApplyDirectories) Commit(transactionOk bool) {
	defer close(op.err)
	if transactionOk == false {
		op.err <- failedTransactionError()
	}
}

func (op *ApplyDirectories) FirstError() error {
	for e := range op.err {
		if e != nil {
			return e
		}
	}
	return nil
}

func (op *ApplyDirectories) RunOutsideTransaction() bool { return false }

type fixPerms struct{ path string }

func (op *fixPerms) Execute() error  { return applyDirectory.FixPerms(op.path) }
func (op *fixPerms) Paths() []string { return []string{op.path} }

type FixPerms struct {
	*InternalFsOperation
}

func NewFixPerms(path string) *FixPerms {
	internal := &fixPerms{path}
	return &FixPerms{newInternalFsOperation(internal, 2)}
}

type makeSingularityEnv struct{ path string }

func (op *makeSingularityEnv) Execute() error  { return singularity.MakeBaseEnv(op.path) }
func (op *makeSingularityEnv) Paths() []string { return []string{op.path} }

type MakeSingularityEnv struct{ *InternalFsOperation }

func NewMakeSingularityEnv(path string) *MakeSingularityEnv {
	internal := &makeSingularityEnv{path}
	return &MakeSingularityEnv{newInternalFsOperation(internal, 2)}
}

type CreateRegularFile struct{ *InternalFsOperation }

func NewCreateRegularFile(path string, content []byte) *CreateRegularFile {
	internal := &createRegularFile{path, content}
	return &CreateRegularFile{newInternalFsOperation(internal, 2)}
}

type createRegularFile struct {
	path    string
	content []byte
}

func (op *createRegularFile) Execute() error {
	if err := os.RemoveAll(op.path); err != nil {
		return err
	}
	dir := filepath.Dir(op.path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	f, err := os.Create(op.path)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := f.Write(op.content); err != nil {
		os.RemoveAll(op.path)
		return err
	}
	return nil
}
func (op *createRegularFile) Paths() []string { return []string{op.path} }
