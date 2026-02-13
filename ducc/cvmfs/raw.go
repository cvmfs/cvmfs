package cvmfs

import (
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	exec "github.com/cvmfs/ducc/exec"
	l "github.com/cvmfs/ducc/log"
	log "github.com/sirupsen/logrus"
)

type TransactionOption interface {
	ToString() string
}

type TemplateTransaction struct {
	source      string
	destination string
}

func NewTemplateTransaction(source, destination string) TemplateTransaction {
	return TemplateTransaction{source, destination}
}

func (t TemplateTransaction) ToString() string {
	return fmt.Sprintf("-T %s=%s", t.source, t.destination)
}

var locksMap = make(map[string]*sync.Mutex)
var locksFile = make(map[string]fSLock)
var lockMap = &sync.Mutex{}

func lockRepoKey(CVMFSRepo string) string {
	repoName, _ := GetRepoAndSubdir(CVMFSRepo)
	return repoName
}

func transactionRepoName(CVMFSRepo string) string {
	repoName, _ := GetRepoAndSubdir(CVMFSRepo)
	return repoName
}

func getLock(CVMFSRepo string) {
	key := lockRepoKey(CVMFSRepo)
	lockMap.Lock()
	lc := locksMap[key]
	if lc == nil {
		locksMap[key] = &sync.Mutex{}
		lc = locksMap[key]
	}
	f := locksFile[key]
	if f == nil {
		f = newFSLock("/tmp/DUCC.lock")
		locksFile[key] = f
		f = locksFile[key]
	}
	lockMap.Unlock()

	lc.Lock()
	err := f.LockWriteB()
	for err != nil {
		// this may happen if the kernel detect a deadlock
		// it should never happen in our case, (of a single global lock)
		// but still we can protect against it
		l.LogE(err).Info("Error in getting the FS lock")
		time.Sleep(100 * time.Millisecond)
		err = f.LockWriteB()
	}
}

func unlock(CVMFSRepo string) {
	key := lockRepoKey(CVMFSRepo)
	lockMap.Lock()
	l := locksMap[key]
	f := locksFile[key]
	lockMap.Unlock()

	l.Unlock()
	f.Unlock()
}

func ExecuteAndOpenTransaction(CVMFSRepo string, f func() error, options ...TransactionOption) error {
	cmd := []string{"cvmfs_server", "transaction"}
	for _, opt := range options {
		cmd = append(cmd, opt.ToString())
	}
	cmd = append(cmd, transactionRepoName(CVMFSRepo))
	getLock(CVMFSRepo)
	if err := f(); err != nil {
		unlock(CVMFSRepo)
		return err
	}
	err := exec.ExecCommand(cmd...).Start()
	if err != nil {
		l.LogE(err).WithFields(
			log.Fields{"repo": CVMFSRepo}).
			Error("Error in opening the transaction")
		Abort(CVMFSRepo)
	}
	return err

}

func OpenTransaction(CVMFSRepo string, options ...TransactionOption) error {
	return ExecuteAndOpenTransaction(CVMFSRepo, func() error { return nil }, options...)
}

func Publish(CVMFSRepo string) error {
	repoName, _ := GetRepoAndSubdir(CVMFSRepo)
	defer unlock(CVMFSRepo)
	err := exec.ExecCommand("cvmfs_server", "publish", repoName).Start()
	if err != nil {
		l.LogE(err).WithFields(
			log.Fields{"repo": CVMFSRepo}).
			Error("Error in publishing the repository")
		abort(CVMFSRepo)
		return err
	}

	l.LogE(err).WithFields(
		log.Fields{"repo": CVMFSRepo}).
		Info("Publish complete")
	return nil
}

func Abort(CVMFSRepo string) error {
	defer unlock(CVMFSRepo)
	err := abort(CVMFSRepo)
	if err != nil {
		l.LogE(err).WithFields(
			log.Fields{"repo": CVMFSRepo}).
			Error("Error in abort the transaction")
	}
	return err
}

func abort(CVMFSRepo string) error {
	repoName, _ := GetRepoAndSubdir(CVMFSRepo)
	return exec.ExecCommand("cvmfs_server", "abort", "-f", repoName).Start()
}

func RepositoryExists(CVMFSRepo string) bool {
	cmd := exec.ExecCommand("cvmfs_server", "list")
	err, stdout, _ := cmd.StartWithOutput()
	if err != nil {
		l.LogE(fmt.Errorf("Error in listing the repository")).
			Error("Repo not present")
		return false
	}
	stdoutString := string(stdout.Bytes())

	// remove sub directory in case it was passed
	repo, _ := GetRepoAndSubdir(CVMFSRepo)
	if strings.Contains(stdoutString, repo) {
		return true
	} else {
		return false
	}
}

func WithinTransaction(CVMFSRepo string, f func() error, opts ...TransactionOption) error {
	err := OpenTransaction(CVMFSRepo, opts...)
	if err != nil {
		return err
	}
	err = f()
	if err != nil {
		return Abort(CVMFSRepo)
	}
	return Publish(CVMFSRepo)
}

func Ingest(CVMFSRepo string, input io.ReadCloser, options ...string) error {
	repoName, _ := GetRepoAndSubdir(CVMFSRepo)
	cmd := []string{"cvmfs_server", "ingest"}
	for _, opt := range options {
		cmd = append(cmd, opt)
	}
	cmd = append(cmd, repoName)
	getLock(CVMFSRepo)
	defer unlock(CVMFSRepo)
	return exec.ExecCommand(cmd...).StdIn(input).Start()
}

func IngestDelete(CVMFSRepo string, path string) error {
	repoName, _ := GetRepoAndSubdir(CVMFSRepo)
	path = PrefixRepoSubdirOnce(CVMFSRepo, path)
	getLock(CVMFSRepo)
	defer unlock(CVMFSRepo)
	return exec.ExecCommand("cvmfs_server", "ingest", "--delete", path, repoName).Start()
}
