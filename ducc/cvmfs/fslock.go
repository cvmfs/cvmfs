package cvmfs

import (
	"os"

	l "github.com/cvmfs/ducc/log"
	"github.com/rubyist/lockfile"
)

type fSLock interface {
	LockWriteB() error
	Unlock()
}

type dummyFSLock struct {
	path string

	realLock *lockfile.FcntlLockfile
}

func (d dummyFSLock) LockWriteB() error {
	if d.realLock == nil {
		file, err := os.OpenFile(d.path, os.O_CREATE|os.O_RDWR, 0666)
		if err != nil {
			l.Log().Warning("Impossible to create FS level lock, DO NOT run multiple process")
			return nil
		}
		d.realLock = lockfile.NewFcntlLockfileFromFile(file)
	}
	if d.realLock == nil {
		return nil
	}
	return d.realLock.LockWriteB()
}

func (d dummyFSLock) Unlock() {
	if d.realLock != nil {
		d.realLock.Unlock()
	}
}

func newFSLock(path string) fSLock {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		l.LogE(err).Warning("Impossible to create FS level lock, DO NOT run multiple process")
		return dummyFSLock{path: path, realLock: nil}
	}
	return lockfile.NewFcntlLockfileFromFile(file)
}
