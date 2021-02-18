// Package lockfile provides a simple wrapper around a system's
// file locking capabilities. It provides wrappers around both
// flock and fcntl type locks. Fcntl based locks are only available
// with go 1.3 or later.
//
// Both flock and fcntl based locks can be explicitly created. lockfile
// also provides a generic creator which will use fcntl if it is available,
// otherwise it will provide a flock based lock.
//
// Here is an example using the generic creator to lock a file for
// writing.
//
//      lock := NewLockfile("myfile.lock")
//      err := lock.LockWrite()
//      if err != nil {
//              log.Fatal(err)
//      }
//      lock.Unlock()
//
// Here is an example of creating an flock based lock file.
//
//      lock := NewFLockfile("myfile.lock")
//      err := lock.LockWrite()
//      if err != nil {
//              log.Fatal(err)
//      }
//      lock.Unlock()
//
// Here is an example of creating a fcntl based lock file. This function is
// only available if the system supports it and Go is version 1.3 or greater.
//
//      lock := NewFcntlLockfile("myfile.lock")
//      err := lock.LockWrite()
//      if err != nil {
//              log.Fatal(err)
//      }
//      lock.Unlock()
//
// Both functions return a lock that conforms to the Locker interface.
package lockfile

import (
	"errors"
)

// Locker is the interface that wraps file locking functionality.
//
// LockRead locks the file for reading. When a file is locked for
// reading, other processes may lock the file for reading, but are
// unable to lock the file for writing. If LockRead cannot obtain
// the lock it will return an error.
//
// LockWrite locks the file for writing. When a file is locked for
// writing, other processes cannot obtain read or write locks on the
// file. If LockWrite cannot obtain the lock it will return an error.
//
// LockReadB is a blocking version of LockRead. If it cannot obtain
// the lock it will block until it is able to.
//
// LockWriteB is a blocking version of LockWrite. If it cannot obtain
// the lock it will block until it is able to.
//
// Unlock releases the lock on the file.
type Locker interface {
	LockRead() error
	LockWrite() error
	LockReadB() error
	LockWriteB() error
	Unlock()
}

var (
	ErrFailedToLock = errors.New("failed to obtain lock")
)
