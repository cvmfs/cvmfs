package singularity

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	log "github.com/sirupsen/logrus"

	exec "github.com/cvmfs/ducc/exec"
	l "github.com/cvmfs/ducc/log"
)

func AssureValidSingularity() error {
	err, stdout, _ := exec.ExecCommand("singularity", "version").StartWithOutput()
	if err != nil {
		err := fmt.Errorf("No working version of Singularity: %s", err)
		l.LogE(err).Error("No working version of Singularity")
		return err
	}
	version := stdout.String()
	sems := strings.Split(version, ".")
	if len(sems) < 2 {
		err := fmt.Errorf("Singularity version returned an unexpected format, unable to find Major and Minor number")
		l.LogE(err).WithFields(log.Fields{"version": version}).Error("Not valid singularity")
		return err
	}
	majorS := sems[0]
	majorI, err := strconv.Atoi(majorS)
	if err != nil {
		errF := fmt.Errorf("Singularity version returned an unexpected format, unable to parse Major number: %s", err)
		l.LogE(errF).WithFields(log.Fields{"version": version, "major number": majorS}).Error("Not valid singularity")
		return errF
	}
	if majorI >= 4 {
		return nil
	}
	minorS := sems[1]
	minorI, err := strconv.Atoi(minorS)
	if err != nil {
		errF := fmt.Errorf("Singularity version returned an unexpected format, unable to parse Minor number: %s", err)
		l.LogE(errF).WithFields(log.Fields{"version": version, "minor number": minorS}).Error("Not valid singularity")
		return errF
	}
	if majorI >= 3 && minorI >= 5 {
		return nil
	}
	errF := fmt.Errorf("Installed singularity is too old, we need at least 3.5: Installed version: %s", version)
	l.LogE(errF).WithFields(log.Fields{"version": version}).Error("Too old singularity")
	return errF
}

func BuildFilesystemDirectory(directoryPath, singularityURL string, env map[string]string) error {
	cmd := exec.ExecCommand("singularity", "build", "--force", "--fix-perms",
		"--sandbox", directoryPath, singularityURL).
		Env("PATH", os.Getenv("PATH"))
	for key, value := range env {
		cmd.Env(key, value)
	}
	return cmd.Start()
}
