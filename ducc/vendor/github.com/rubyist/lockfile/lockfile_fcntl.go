// +build linux darwin freebsd openbsd netbsd dragonfly
// +build go1.3

package lockfile

import (
	"fmt"
	"os"
	"syscall"
)

type FcntlLockfile struct {
	Path         string
	file         *os.File
	maintainFile bool
	ft           *syscall.Flock_t
}

func NewFcntlLockfile(path string) *FcntlLockfile {
	return &FcntlLockfile{Path: path, maintainFile: true}
}

func NewFcntlLockfileFromFile(file *os.File) *FcntlLockfile {
	return &FcntlLockfile{file: file, maintainFile: false}
}

func (l *FcntlLockfile) LockRead() error {
	return l.lock(false, false, 0, os.SEEK_SET, 0)
}

func (l *FcntlLockfile) LockWrite() error {
	return l.lock(true, false, 0, os.SEEK_SET, 0)
}

func (l *FcntlLockfile) LockReadB() error {
	return l.lock(false, true, 0, os.SEEK_SET, 0)
}

func (l *FcntlLockfile) LockWriteB() error {
	return l.lock(true, true, 0, os.SEEK_SET, 0)
}

func (l *FcntlLockfile) Unlock() {
	l.unlock(0, os.SEEK_SET, 0)
}

func (l *FcntlLockfile) LockReadRange(offset int64, whence int, len int64) error {
	return l.lock(false, false, offset, whence, len)
}

func (l *FcntlLockfile) LockWriteRange(offset int64, whence int, len int64) error {
	return l.lock(true, false, offset, whence, len)
}

func (l *FcntlLockfile) LockReadRangeB(offset int64, whence int, len int64) error {
	return l.lock(false, true, offset, whence, len)
}

func (l *FcntlLockfile) LockWriteRangeB(offset int64, whence int, len int64) error {
	return l.lock(true, true, offset, whence, len)
}

func (l *FcntlLockfile) UnlockRange(offset int64, whence int, len int64) {
	l.unlock(offset, whence, len)
}

// Owner will return the pid of the process that owns an fcntl based
// lock on the file. If the file is not locked it will return -1. If
// a lock is owned by the current process, it will return -1.
func (l *FcntlLockfile) Owner() int {
	ft := &syscall.Flock_t{}
	*ft = *l.ft

	err := syscall.FcntlFlock(l.file.Fd(), syscall.F_GETLK, ft)
	if err != nil {
		fmt.Println(err)
		return -1
	}

	if ft.Type == syscall.F_UNLCK {
		fmt.Println(err)
		return -1
	}

	return int(ft.Pid)
}

func (l *FcntlLockfile) lock(exclusive, blocking bool, offset int64, whence int, len int64) error {
	if l.file == nil {
		f, err := os.OpenFile(l.Path, os.O_CREATE|os.O_RDWR, 0666)
		if err != nil {
			return err
		}
		l.file = f
	}

	ft := &syscall.Flock_t{
		Whence: int16(whence),
		Start:  offset,
		Len:    len,
		Pid:    int32(os.Getpid()),
	}
	l.ft = ft

	if exclusive {
		ft.Type = syscall.F_WRLCK
	} else {
		ft.Type = syscall.F_RDLCK
	}
	var flags int
	if blocking {
		flags = syscall.F_SETLKW
	} else {
		flags = syscall.F_SETLK
	}

	err := syscall.FcntlFlock(l.file.Fd(), flags, l.ft)
	if err != nil {
		if l.maintainFile {
			l.file.Close()
		}
		return ErrFailedToLock
	}

	return nil
}

func (l *FcntlLockfile) unlock(offset int64, whence int, len int64) {
	l.ft.Len = len
	l.ft.Start = offset
	l.ft.Whence = int16(whence)
	l.ft.Type = syscall.F_UNLCK
	syscall.FcntlFlock(l.file.Fd(), syscall.F_SETLK, l.ft)
	if l.maintainFile {
		l.file.Close()
	}
}
