package daemon

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

const daemonLockFilePath = "/tmp/cvmfs/ducc/daemon.lock"

var daemonLockFile *os.File

func acquireDaemonLock() error {
	var err error

	if err := os.MkdirAll(filepath.Dir(daemonLockFilePath), 0755); err != nil {
		return fmt.Errorf("error creating daemon lock directory: %s", err)
	}
	daemonLockFile, err = os.OpenFile(daemonLockFilePath, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return fmt.Errorf("error opening daemon lock file: %s", err)
	}

	// Get exclusive lock on file
	if err := syscall.Flock(int(daemonLockFile.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return fmt.Errorf("error locking daemon lock file: %s", err)
	}

	pid := os.Getpid()
	daemonLockFile.Truncate(0)
	daemonLockFile.WriteString(fmt.Sprintf("%d", pid))

	return nil
}

func releaseDaemonLock() {
	if daemonLockFile != nil {
		daemonLockFile.Close()
		os.Remove(daemonLockFilePath)
	}
}
