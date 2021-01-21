package lockfile

import (
	"os"
	"syscall"
)

type FLockfile struct {
	Path         string
	file         *os.File
	lockObtained bool
	maintainFile bool
}

func NewFLockfile(path string) *FLockfile {
	return &FLockfile{Path: path, maintainFile: true}
}

func NewFLockfileFromFile(file *os.File) *FLockfile {
	return &FLockfile{file: file, maintainFile: false}
}

func (l *FLockfile) LockRead() error {
	return l.lock(false, false)
}

func (l *FLockfile) LockWrite() error {
	return l.lock(true, false)
}

func (l *FLockfile) LockReadB() error {
	return l.lock(false, true)
}

func (l *FLockfile) LockWriteB() error {
	return l.lock(true, true)
}

func (l *FLockfile) Unlock() {
	if !l.lockObtained {
		return
	}

	syscall.Flock(int(l.file.Fd()), syscall.LOCK_UN)
	if l.maintainFile {
		l.file.Close()
	}
}

func (l *FLockfile) lock(exclusive, blocking bool) error {
	if l.file == nil {
		f, err := os.OpenFile(l.Path, os.O_CREATE|os.O_RDWR, 0666)
		if err != nil {
			return err
		}
		l.file = f
	}

	var flags int
	if exclusive {
		flags = syscall.LOCK_EX
	} else {
		flags = syscall.LOCK_SH
	}
	if !blocking {
		flags |= syscall.LOCK_NB
	}

	err := syscall.Flock(int(l.file.Fd()), flags)
	if err != nil {
		if l.maintainFile {
			l.file.Close()
		}
		return ErrFailedToLock
	}

	l.lockObtained = true

	return nil
}
