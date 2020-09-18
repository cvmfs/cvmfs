package cvmfs

import (
	"fmt"
	"os"
	"os/user"
	"path/filepath"
	"strings"
	"sync"

	"github.com/cvmfs/ducc/lib"
)

type Repository struct {
	Name           string
	FSLock         sync.Mutex
	fsoperations   []FSOperation
	operationsLock sync.Mutex
}

func NewRepository(name string) Repository {
	return Repository{name, sync.Mutex{}, make([]FSOperation, 0), sync.Mutex{}}
}

func (repo *Repository) Root() string {
	return filepath.Join("/", "cvmfs", repo.Name)
}

func (repo *Repository) AddFSOperation(ops FSOperation) error {
	basePath := filepath.Join("/", "cvmfs", repo.Name)
	for _, p := range ops.Paths() {
		if !strings.HasPrefix(p, basePath) {
			return fmt.Errorf("error: the operation acts against a path not in the repository: %s", p)
		}
	}
	repo.operationsLock.Lock()
	defer repo.operationsLock.Unlock()
	repo.fsoperations = append(repo.fsoperations, ops)
	return nil
}

func (repo *Repository) ExecuteFSOperations() (error, []error) {
	repo.operationsLock.Lock()
	operations := make([]FSOperation, 0, len(repo.fsoperations))
	i := 0
	for _, ops := range repo.fsoperations {
		if i > 30 {
			break
		}
		if ops.RunInPrivateTransaction() {
			if i > 0 {
				break
			}
			operations = append(operations, ops)
			i += 1
			break
		}
		operations = append(operations, ops)
		i += 1
	}

	repo.fsoperations = repo.fsoperations[i:]

	repo.operationsLock.Unlock()

	errors := make([]error, 0, len(operations))
	opsErr := error(nil)
	if err := repo.Transaction(); err != nil {
		return err, errors
	}

	for _, operation := range operations {
		opsErr = operation.Execute(opsErr)
		errors = append(errors, opsErr)
	}

	if err := repo.Publish(); err != nil {
		return err, errors
	}
	return nil, errors
}

func (repo *Repository) MkFsWithUser(owner string) error {
	return lib.ExecCommand("sudo", "cvmfs_server", "mkfs", "-o", owner, repo.Name).Start()
}

func (repo *Repository) MkFs() error {
	user, err := user.Current()
	if err != nil {
		return err
	}
	return repo.MkFsWithUser(user.Username)
}

func (repo *Repository) RmFs() error {
	return lib.ExecCommand("sudo", "cvmfs_server", "rmfs", "-f", repo.Name).Start()
}

func (repo *Repository) Transaction() error {
	repo.FSLock.Lock()
	return lib.ExecCommand("cvmfs_server", "transaction", repo.Name).Start()
}

func (repo *Repository) Publish() error {
	defer repo.FSLock.Unlock()
	return lib.ExecCommand("cvmfs_server", "publish", repo.Name).Start()
}

func (repo *Repository) Abort() error {
	defer repo.FSLock.Unlock()
	return lib.ExecCommand("cvmfs_server", "abort", "-f", repo.Name).Start()
}

type FSOperation interface {
	Execute(previousError error) error
	Paths() []string
	RunInPrivateTransaction() bool
	RunOutsideTransaction() bool
}

type CreateDirectory struct {
	path string
}

func (c CreateDirectory) Execute(prevError error) error {
	return os.MkdirAll(c.path, 0755)
}

func (c CreateDirectory) Paths() []string {
	return []string{c.path}
}

func (c CreateDirectory) RunInPrivateTransaction() bool { return false }
func (c CreateDirectory) RunOutsideTransaction() bool   { return false }
