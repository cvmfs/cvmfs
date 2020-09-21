package cvmfs

import (
	"fmt"
	"os"
	"os/user"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cvmfs/ducc/lib"
)

type Repository struct {
	Name           string
	FSLock         sync.Mutex
	fsoperations   chan FSOperation
	operationsLock sync.Mutex
	opsIndex       uint64
	doneIndex      uint64
}

func NewRepository(name string) Repository {
	return Repository{name, sync.Mutex{}, make(chan FSOperation, 50), sync.Mutex{}, 0, 0}
}

func (repo *Repository) Root() string {
	return filepath.Join("/", "cvmfs", repo.Name)
}

func (repo *Repository) AddFSOperations(ops ...FSOperation) (uint64, error) {
	basePath := filepath.Join("/", "cvmfs", repo.Name)
	for _, op := range ops {
		for _, p := range op.Paths() {
			if !strings.HasPrefix(p, basePath) {
				return repo.opsIndex, fmt.Errorf("error: the operation acts against a path not in the repository: %s", p)
			}
		}
	}
	repo.operationsLock.Lock()
	defer repo.operationsLock.Unlock()
	for _, op := range ops {
		repo.fsoperations <- op
	}
	// we are inside a mutex, it is not strictly necessary to use atomic
	atomic.AddUint64(&repo.opsIndex, uint64(len(ops)))
	return repo.opsIndex, nil
}

func (repo *Repository) DoneIndex() uint64 {
	return atomic.LoadUint64(&repo.doneIndex)
}

func (repo *Repository) Execs() {
	scrathSpace := make([]FSOperation, 0, 30)

	txtOpen := false
	err := repo.Transaction()
	if err != nil {
		// XXX this is somehow a problem
		txtOpen = false
	}

	abortAndCleanup := func() {
		repo.Abort()
		for _, op := range scrathSpace {
			op.Commit(false)
		}
		atomic.AddUint64(&repo.doneIndex, uint64(len(scrathSpace)))
		scrathSpace = make([]FSOperation, 0, 30)
	}

	for {
		select {
		case <-time.After(1 * time.Second):
			if txtOpen {
				err = repo.Publish()
				txtOpen = false
				for _, op := range scrathSpace {
					op.Commit(err == nil)
				}
				atomic.AddUint64(&repo.doneIndex, uint64(len(scrathSpace)))
			}

		case ops := <-repo.fsoperations:

			switch txtOpen {

			case true: // INSIDE transaction
				switch ops.RunOutsideTransaction() {
				case false: // normal operation
					err := ops.Execute()
					if err != nil {
						abortAndCleanup()
					} else {
						scrathSpace = append(scrathSpace, ops)
					}

				case true: // ingest operation

					// first close the transaction
					transactionOk := true
					err := repo.Publish()
					txtOpen = false
					if err != nil {
						transactionOk = false
					}

					// then we communicate if the transaction was committed
					for _, ops := range scrathSpace {
						ops.Commit(transactionOk)
					}
					atomic.AddUint64(&repo.doneIndex, uint64(len(scrathSpace)))
					scrathSpace = make([]FSOperation, 0, 30)

					// finally we execute the new operation
					err = ops.Execute()
					ops.Commit(err == nil)
					atomic.AddUint64(&repo.doneIndex, 1)
				}

			case false: // NOT in transaction
				switch ops.RunOutsideTransaction() {
				case false: // normal operation
					err := repo.Transaction()
					txtOpen = true
					if err != nil {
						txtOpen = false
					}
					err = ops.Execute()
					if err != nil {
						abortAndCleanup()
					} else {
						scrathSpace = append(scrathSpace, ops)
					}

				case true: // ingest operation
					err := ops.Execute()
					ops.Commit(err == nil)
					atomic.AddUint64(&repo.doneIndex, 1)
				}

			}

			if len(scrathSpace) == cap(scrathSpace) {
				err := repo.Publish()
				txtOpen = false
				for _, op := range scrathSpace {
					op.Commit(err == nil)
				}
				atomic.AddUint64(&repo.doneIndex, uint64(len(scrathSpace)))
				scrathSpace = make([]FSOperation, 0, 30)
			}

		}

	}
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
	Execute() error
	Paths() []string
	Commit(transactionOk bool)
	RunInPrivateTransaction() bool
	RunOutsideTransaction() bool
}

type CreateDirectory struct {
	path string
}

func (c CreateDirectory) Execute() error {
	return os.MkdirAll(c.path, 0755)
}

func (c CreateDirectory) Paths() []string {
	return []string{c.path}
}

func (c CreateDirectory) Commit(txtOk bool) {}

func (c CreateDirectory) RunInPrivateTransaction() bool { return false }
func (c CreateDirectory) RunOutsideTransaction() bool   { return false }
