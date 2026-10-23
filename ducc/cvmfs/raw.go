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
		l.LogE(err).Warning("Error in getting the FS lock")
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
	return ExecuteAndOpenTransactionWithLogger(nil, CVMFSRepo, f, options...)
}

func ExecuteAndOpenTransactionWithLogger(logger *log.Entry, CVMFSRepo string, f func() error, options ...TransactionOption) error {
	logger = l.Ensure(logger)
	transactionLogger := logger.WithFields(log.Fields{"repo": CVMFSRepo, "action": "transaction"})
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
	err := exec.ExecCommandWithLogger(transactionLogger, cmd...).Start()
	if err != nil {
		transactionLogger.WithField("error", err).Error("Error in opening the transaction")
		AbortWithLogger(logger, CVMFSRepo)
	}
	return err

}

func OpenTransaction(CVMFSRepo string, options ...TransactionOption) error {
	return OpenTransactionWithLogger(nil, CVMFSRepo, options...)
}

func OpenTransactionWithLogger(logger *log.Entry, CVMFSRepo string, options ...TransactionOption) error {
	return ExecuteAndOpenTransactionWithLogger(logger, CVMFSRepo, func() error { return nil }, options...)
}

func Publish(CVMFSRepo string) error {
	return PublishWithLogger(nil, CVMFSRepo)
}

func PublishWithLogger(logger *log.Entry, CVMFSRepo string) error {
	logger = l.Ensure(logger)
	publishLogger := logger.WithFields(log.Fields{"repo": CVMFSRepo, "action": "publish"})
	repoName, _ := GetRepoAndSubdir(CVMFSRepo)
	defer unlock(CVMFSRepo)
	err := exec.ExecCommandWithLogger(publishLogger, "cvmfs_server", "publish", repoName).Start()
	if err != nil {
		publishLogger.WithField("error", err).Error("Error in publishing the repository")
		abortWithLogger(logger, CVMFSRepo)
		return err
	}

	publishLogger.Trace("Publish complete")
	return nil
}

func Abort(CVMFSRepo string) error {
	return AbortWithLogger(nil, CVMFSRepo)
}

func AbortWithLogger(logger *log.Entry, CVMFSRepo string) error {
	defer unlock(CVMFSRepo)
	err := abortWithLogger(logger, CVMFSRepo)
	if err != nil {
		l.Ensure(logger).WithFields(log.Fields{"repo": CVMFSRepo, "action": "abort", "error": err}).
			Error("Error in abort the transaction")
	}
	return err
}

func abort(CVMFSRepo string) error {
	return abortWithLogger(nil, CVMFSRepo)
}

func abortWithLogger(logger *log.Entry, CVMFSRepo string) error {
	logger = l.Ensure(logger)
	abortLogger := logger.WithFields(log.Fields{"repo": CVMFSRepo, "action": "abort"})
	repoName, _ := GetRepoAndSubdir(CVMFSRepo)
	return exec.ExecCommandWithLogger(abortLogger, "cvmfs_server", "abort", "-f", repoName).Start()
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
	return repositoryExistsInList(stdoutString, repo)
}

func repositoryExistsInList(stdoutString, repo string) bool {
	candidates := map[string]struct{}{
		repo: {},
	}
	if strings.HasPrefix(repo, "/") {
		candidates[strings.TrimPrefix(repo, "/")] = struct{}{}
		candidates[".."+repo] = struct{}{}
	}
	if strings.HasPrefix(repo, "..") {
		candidates[strings.TrimPrefix(repo, "..")] = struct{}{}
	}

	for _, line := range strings.Split(stdoutString, "\n") {
		line = strings.TrimSpace(line)
		if _, ok := candidates[line]; ok {
			return true
		}
		fields := strings.Fields(line)
		if len(fields) > 0 {
			if _, ok := candidates[fields[0]]; ok {
				return true
			}
		}
	}
	return false
}

func WithinTransaction(CVMFSRepo string, f func() error, opts ...TransactionOption) error {
	return WithinTransactionWithLogger(nil, CVMFSRepo, f, opts...)
}

func WithinTransactionWithLogger(logger *log.Entry, CVMFSRepo string, f func() error, opts ...TransactionOption) error {
	err := OpenTransactionWithLogger(logger, CVMFSRepo, opts...)
	if err != nil {
		return err
	}
	err = f()
	if err != nil {
		return AbortWithLogger(logger, CVMFSRepo)
	}
	return PublishWithLogger(logger, CVMFSRepo)
}

func Ingest(CVMFSRepo string, input io.ReadCloser, options ...string) error {
	return IngestWithLogger(nil, CVMFSRepo, input, options...)
}

func IngestWithLogger(logger *log.Entry, CVMFSRepo string, input io.ReadCloser, options ...string) error {
	logger = l.Ensure(logger)
	ingestLogger := logger.WithFields(log.Fields{"repo": CVMFSRepo, "action": "ingest"})
	repoName, _ := GetRepoAndSubdir(CVMFSRepo)
	cmd := []string{"cvmfs_server", "ingest"}
	for _, opt := range options {
		cmd = append(cmd, opt)
	}
	cmd = append(cmd, repoName)
	getLock(CVMFSRepo)
	defer unlock(CVMFSRepo)
	return exec.ExecCommandWithLogger(ingestLogger, cmd...).StdIn(input).Start()
}

func IngestDelete(CVMFSRepo string, path string) error {
	return IngestDeleteWithLogger(nil, CVMFSRepo, path)
}

func IngestDeleteWithLogger(logger *log.Entry, CVMFSRepo string, path string) error {
	logger = l.Ensure(logger)
	// Mountless gateway publishing supports only --fast-delete: regular
	// --delete needs the rdonly mount for filesystem traversal.
	if MountlessPublishing() {
		return IngestFastDeleteWithLogger(logger, CVMFSRepo, path)
	}
	ingestLogger := logger.WithFields(log.Fields{"repo": CVMFSRepo, "action": "ingest delete", "path": path})
	repoName, _ := GetRepoAndSubdir(CVMFSRepo)
	path = PrefixRepoSubdirOnce(CVMFSRepo, path)
	getLock(CVMFSRepo)
	defer unlock(CVMFSRepo)
	return exec.ExecCommandWithLogger(ingestLogger, "cvmfs_server", "ingest", "--delete", path, repoName).Start()
}

func IngestFastDelete(CVMFSRepo string, path string) error {
	return IngestFastDeleteWithLogger(nil, CVMFSRepo, path)
}

func IngestFastDeleteWithLogger(logger *log.Entry, CVMFSRepo string, path string) error {
	logger = l.Ensure(logger)
	ingestLogger := logger.WithFields(log.Fields{"repo": CVMFSRepo, "action": "ingest fast-delete", "path": path})
	repoName, _ := GetRepoAndSubdir(CVMFSRepo)
	path = PrefixRepoSubdirOnce(CVMFSRepo, path)
	getLock(CVMFSRepo)
	defer unlock(CVMFSRepo)
	return exec.ExecCommandWithLogger(ingestLogger, "cvmfs_server", "ingest", "--fast-delete", path, repoName).Start()
}
