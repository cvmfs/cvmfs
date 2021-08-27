# lockfile

Lockfile provides a simple wrapper for `fcntl` and `flock` based advisory file locking.

## Installation

```
go get github.com/rubyist/lockfile
```

## Examples

`lockfile` provides a function `NewLockfile()` which will use an `fcntl` based lock, if
it is available on the system. `fcntl` based locks are available in Go 1.3 and later. If
`fcntl` is not available it will fall back to an `flock` based lock.

```go
lock := lockfile.NewLockfile("myfile")
```

A `fcntl` based lock can be built explicitly:

```go
lock := lockfile.NewFcntlLockfile("myfile")
```

A `flock` based lock can be built explicitly:

```go
lock := lockfile.NewFLockfile("myfile")
```

An `os.File` can also be used.

```go
file, _ := os.Open("myfile")
lock := lockfile.NewLockfileFromFile(file)
lock2 := lockfile.NewFcntlLockfileFromFile(file)
lock3 := lockfile.NewFLockfileFromFile(file)
```

### Read Locks

The `LockRead()` function will lock a file for reading. When a file is locked for reading,
other processes will be able to obtain read locks on the file. Write locks cannot be
obtained on files locked for reading. If the lock cannot be obtained, `LockRead()` will
return an error immediately

The `LockReadB()` function provides the same read locking functionality but will block
until the lock can be obtained.

```go
err := lock.LockRead()
if err != nil {
  // Lock not obtained
}

lock.LockReadB() // blocks until lock can be obtained
```

### Write Locks

The `LockWrite()` function will lock a file for writing. When a file is locked for writing,
other processes will not be able to obtain read or write locks on the file. If the lock
cannot be obtained, `LockWrite()` will return an error immediately.

The `LockWriteB()` function provides the same write locking functionality but will block
until the lock can be obtained.

```go
err := lock.LockWrite()
if err != nil {
  // Lock not obtained
}

lock.LockWriteB() // blocks until the lock can be obtained
```

### Releasing locks

The `Unlock()` function will release a lock. Closing a file or exiting the process
will also release any locks held on the file.

*Caveat*: If the locking process opens a locked file and then closes it, all locks
on that file will be released.

```go
lock := lockfile.NewLockfile("foobar.lock")
lock.LockWrite() // lock obtained

f, _ := os.Open("foobar.lock")
f.Close() // lock released
```

### Locking Byte Ranges

When using an fcntl based lock, byte ranges within the file can be locked and unlocked.
The functions availble to do this are:

```go
LockReadRange(offset int64, whence int, len int64) error
LockWriteRange(offset int64, whence int, len int64) error
LockReadRangeB(offset int64, whence int, len int64) error
LockWriteRangeB(offset int64, whence int, len int64) error
UnlockRange(offset int64, whence int, len int64)
```

- whence: 0, 1, or 2 where:
  - 0: relative to the origin of the file
  - 1: relative to the current offset of the file
  - 2: relative to the end of the file
- offset: The offset, in bytes, from whence
- len: The number of bytes to lock

## Bugs and Issues

Issues and pull requests on [GitHub](https://github.com/rubyist/lockfile)
