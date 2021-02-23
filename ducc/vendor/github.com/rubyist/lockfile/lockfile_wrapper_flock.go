// +build go1.2,!go1.3

package lockfile

import (
	"os"
)

// NewLockfile creates a Locker for a file path. The underlying
// Locker created will be either a FLockfile or a FcntlLockfile,
// depending on system capabilities. If the fcntl based function
// is available it will use that, otherwise it will use flock.
func NewLockfile(path string) Locker {
	return NewFLockfile(path)
}

// NewLockfile creates a Locker from an *os.File. The underlying
// Locker created will be either a FLockfile or a FcntlLockfile,
// depending on system capabilities. If the fcntl based function
// is available it will use that, otherwise it will use flock.
//
// The file must be opened with the capabilities needed by the lock.
// e.g. if the file is open for reading only, a write lock cannot
// be obtained.
func NewLockfileFromFile(file *os.File) Locker {
	return NewFLockfileFromFile(file)
}
