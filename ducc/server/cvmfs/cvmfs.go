package cvmfs

import (
	"fmt"
	"os/user"
	"path/filepath"
	"strings"
	"sync"
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
	commitCh       chan bool
	waitingLists   map[uint64][]chan bool
}

func NewRepository(name string) *Repository {
	fsoperations := make(chan FSOperation, 50)
	opsIndex := uint64(0)
	doneIndex := uint64(0)
	commitCh := make(chan bool, 5)
	waitingLists := make(map[uint64][]chan bool)
	repo := &Repository{name,
		sync.Mutex{},
		fsoperations,
		sync.Mutex{},
		opsIndex,
		doneIndex,
		commitCh,
		waitingLists}
	go repo.counter()
	return repo
}

// returns the root of the repository
func (repo *Repository) Root() string {
	return filepath.Join("/", "cvmfs", repo.Name)
}

// add a filesyste operation that should execute against the repository
// if everything goes well it returns an index (and nil error).
// the index can be used with the `WaitFor` method
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
	repo.opsIndex += uint64(len(ops))

	return repo.opsIndex, nil
}

// the index of the last operation done
func (repo *Repository) DoneIndex() uint64 {
	repo.operationsLock.Lock()
	defer repo.operationsLock.Unlock()
	return repo.doneIndex
}

// internal method to signal that an action was executed agains the repository
func (repo *Repository) commit(op FSOperation, transactionOk bool) {
	go op.Commit(transactionOk)
	repo.commitCh <- true
}

// internal method to keep track of the actions executed
// this MUST not fail
func (repo *Repository) counter() {
	for range repo.commitCh {

		repo.operationsLock.Lock()

		repo.doneIndex++
		chs, ok := repo.waitingLists[repo.doneIndex]

		if ok {
			for _, ch := range chs {
				ch <- true
			}
			delete(repo.waitingLists, repo.doneIndex)
		}
		repo.operationsLock.Unlock()
	}
}

// wait for the commit of an action, the target/index to use is the one returned by `AddFSOperations`
func (repo *Repository) WaitFor(target uint64) error {
	repo.operationsLock.Lock()
	// do not use defer to unlock this unless we also manage to wait outside of the function
	// otherwise we will go in a deadlock

	lowerBound := repo.doneIndex
	upperBound := repo.opsIndex
	if target < lowerBound {
		repo.operationsLock.Unlock()
		return NewWaitForExpiredErr(target, lowerBound)
	}
	if target > upperBound {
		repo.operationsLock.Unlock()
		return NewWaitForNotScheduledErr(target, upperBound)
	}

	ch := make(chan bool)
	repo.waitingLists[target] = append(repo.waitingLists[target], ch)
	repo.operationsLock.Unlock()

	// here we wait
	<-ch

	return nil
}

// start to consume the File System operations scheduled agains the repository
func (repo *Repository) StartOperationsLoop() {
	scrathSpace := make([]FSOperation, 0, 30)

	err := repo.Transaction()
	txtOpen := (err == nil)
	if err != nil {
		fmt.Println("Error in opening the transaction??", err)
	}

	abortAndCleanup := func() {
		repo.Abort()
		for _, op := range scrathSpace {
			repo.commit(op, false)
		}
		scrathSpace = make([]FSOperation, 0, 30)
	}

	for {
		select {
		// if we don't receive anything, we commit what we have in the scratch space and we move on
		case <-time.After(1 * time.Second):
			if txtOpen {
				err = repo.Publish()
				txtOpen = false
				for _, op := range scrathSpace {
					repo.commit(op, err == nil)
				}
				scrathSpace = make([]FSOperation, 0, 30)
			}

		case ops := <-repo.fsoperations:

			switch txtOpen {

			case true: // INSIDE transaction
				switch ops.RunOutsideTransaction() {
				case false: // normal operation
					err := ops.Execute()
					scrathSpace = append(scrathSpace, ops)
					if err != nil {
						abortAndCleanup()
					}

				case true: // ingest operation

					// first close the transaction
					err := repo.Publish()
					txtOpen = false

					// then we communicate if the transaction was committed
					for _, op := range scrathSpace {
						repo.commit(op, err == nil)
					}
					scrathSpace = make([]FSOperation, 0, 30)

					// finally we execute the new operation
					err = ops.Execute()
					repo.commit(ops, err == nil)
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
					repo.commit(ops, err == nil)
				}

			}

			if len(scrathSpace) == cap(scrathSpace) {
				err := repo.Publish()
				txtOpen = false
				for _, op := range scrathSpace {
					repo.commit(op, err == nil)
				}
				scrathSpace = make([]FSOperation, 0, 30)
			}

		}

	}
}

// create the new cvmfs file system with the supplied user
func (repo *Repository) MkFsWithUser(owner string) error {
	return lib.ExecCommand("sudo", "cvmfs_server", "mkfs", "-o", owner, repo.Name).Start()
}

// create the new cvmfs file system with the current user
func (repo *Repository) MkFs() error {
	user, err := user.Current()
	if err != nil {
		return err
	}
	return repo.MkFsWithUser(user.Username)
}

// remove the cvmfs file system
func (repo *Repository) RmFs() error {
	return lib.ExecCommand("sudo", "cvmfs_server", "rmfs", "-f", repo.Name).Start()
}

// start a transaction
func (repo *Repository) Transaction() error {
	repo.FSLock.Lock()
	return lib.ExecCommand("cvmfs_server", "transaction", repo.Name).Start()
}

// commit a transaction
func (repo *Repository) Publish() error {
	defer repo.FSLock.Unlock()
	return lib.ExecCommand("cvmfs_server", "publish", repo.Name).Start()
}

// abort a transaction
func (repo *Repository) Abort() error {
	defer repo.FSLock.Unlock()
	return lib.ExecCommand("cvmfs_server", "abort", "-f", repo.Name).Start()
}
