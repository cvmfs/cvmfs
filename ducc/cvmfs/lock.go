package cvmfs

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/cvmfs/ducc/config"
	"github.com/rubyist/lockfile"
)

var lockDirectory = filepath.Join(config.TempDir, "repo_locks")

var locksMap = make(map[string]*sync.Mutex)
var locksFile = make(map[string]lockfile.Locker)
var lockMap = &sync.Mutex{}

func GetLock(CVMFSRepo string) {
	lockMap.Lock()
	lc := locksMap[CVMFSRepo]
	if lc == nil {
		locksMap[CVMFSRepo] = &sync.Mutex{}
		lc = locksMap[CVMFSRepo]
	}
	f := locksFile[CVMFSRepo]
	if f == nil {
		f = lockfile.NewLockfile(filepath.Join(lockDirectory, CVMFSRepo+".lock"))
		locksFile[CVMFSRepo] = f
		f = locksFile[CVMFSRepo]
	}
	lockMap.Unlock()
	lc.Lock()
	err := f.LockWriteB()
	for err != nil {
		// this may happen if the kernel detect a deadlock
		// it should never happen in our case, (of a single global lock)
		// but still we can protect against it
		fmt.Fprintf(os.Stderr, "Error in getting the FS lock:  %s", err)
		time.Sleep(100 * time.Millisecond)
		err = f.LockWriteB()
	}
}

func Unlock(CVMFSRepo string) {
	lockMap.Lock()
	locksMap[CVMFSRepo].Unlock()
	locksFile[CVMFSRepo].Unlock()
	lockMap.Unlock()
}
