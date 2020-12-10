package cvmfs

import (
	"fmt"
	"strings"

	exec "github.com/cvmfs/ducc/exec"
	log "github.com/sirupsen/logrus"
)

func OpenTransaction(CVMFSRepo string) error {
	err := exec.ExecCommand("cvmfs_server", "transaction", CVMFSRepo).Start()
	if err != nil {
		LogE(err).WithFields(log.Fields{"repo": CVMFSRepo}).Error("Error in opening the transaction")
		abort(CVMFSRepo)
	}
	return err
}
func Publish(CVMFSRepo string) error {
	err := exec.ExecCommand("cvmfs_server", "publish", CVMFSRepo).Start()
	if err != nil {
		LogE(err).WithFields(log.Fields{"repo": CVMFSRepo}).Error("Error in publishing the repository")
		abort(CVMFSRepo)
	}
	return err
}

func Abort(CVMFSRepo string) error {
	err := abort(CVMFSRepo)
	if err != nil {
		LogE(err).WithFields(log.Fields{"repo": CVMFSRepo}).Error("Error in abort the transaction")
	}
	return err
}

func abort(CVMFSRepo string) error {
	return exec.ExecCommand("cvmfs_server", "abort", "-f", CVMFSRepo).Start()
}

func RepositoryExists(CVMFSRepo string) bool {
	cmd := exec.ExecCommand("cvmfs_server", "list")
	err, stdout, _ := cmd.StartWithOutput()
	if err != nil {
		LogE(fmt.Errorf("Error in listing the repository")).Error("Repo not present")
		return false
	}
	stdoutString := string(stdout.Bytes())

	if strings.Contains(stdoutString, CVMFSRepo) {
		return true
	} else {
		return false
	}
}
