// Copyright CERN.
//
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

package automount

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	goexec "os/exec"
	"os/signal"
	"path"
	"sync/atomic"
	"syscall"

	"github.com/cvmfs/cvmfs-csi/internal/cvmfs/env"
	"github.com/cvmfs/cvmfs-csi/internal/exec"
	"github.com/cvmfs/cvmfs-csi/internal/log"
)

const (
	AutofsCvmfsRoot = "/cvmfs"
	AlienCachePath  = "/cvmfs-aliencache"
	LocalCachePath  = "/cvmfs-localcache"
)

type Opts struct {
	// Number of seconds of idle time after which an autofs-managed CVMFS
	// mount will be unmounted. Zero means never unmount.
	UnmountTimeoutSeconds int

	// HasAlienCache determines whether we're using alien cache.
	// If so, we need to prepare the alien cache volume first (e.g.
	// make sure it has correct permissions).
	HasAlienCache bool
}

func cvmfsVersion() (string, error) {
	out, err := exec.CombinedOutput(goexec.Command("cvmfs2", "--version"))
	if err != nil {
		return "", fmt.Errorf("failed to get CVMFS version: %v", err)
	}

	return string(bytes.TrimSpace(out)), nil
}

func removeDirContents(dirName string) error {
	contents, err := os.ReadDir(dirName)
	if err != nil {
		// Ignore ENOENT.
		if os.IsNotExist(err) {
			return nil
		}

		return err
	}

	for i := range contents {
		if err = os.RemoveAll(path.Join(dirName, contents[i].Name())); err != nil {
			return err
		}
	}

	return nil
}

func readEffectiveDefaultCvmfsConfig() (map[string]string, error) {
	out, err := exec.Output(goexec.Command(
		"cvmfs_config",
		"showconfig",
		// Show only non-empty config parameters.
		"-s",
		// Repository name. We use "x" as a dummy value,
		// as we don't care at this point, and need only
		// the default, not repository-specific values.
		"x",
	))

	if err != nil {
		execErr, ok := err.(*goexec.ExitError)
		if !ok {
			return nil, err
		}

		// The command normally exits with code 1, because
		// the repository "x" does not exist. The output is
		// still valid.
		if execErr.ExitCode() != 1 {
			return nil, err
		}
	}

	var (
		buf    = bytes.NewBuffer(out)
		sc     = bufio.NewScanner(buf)
		config = make(map[string]string)
	)

	for sc.Scan() {
		// Each line is expected to be in the following format:
		//
		//   <Key>=<Value>
		//
		//  when there is no comment, or
		//
		//   <Key>=<Value>    # from <Source filepath>
		//
		//  when there is a comment.
		line := sc.Bytes()

		// Find the equal sign '=' where the Key is separated from the Value.

		eqTok := bytes.IndexByte(line, '=')
		if eqTok == -1 {
			log.Debugf("Read unexpected CVMFS config parameter \"%s\", missing '='", line)
			continue
		}

		// Cut the comment from the Value, if any.

		valEndIdx := len(line)
		const commentPrefix = "    #"

		if commentTok := bytes.LastIndexByte(line, '#'); commentTok != -1 {
			if bytes.HasSuffix(line[:commentTok+1], []byte(commentPrefix)) {
				valEndIdx = commentTok - len(commentPrefix) + 1
			}
		}

		// Add to the map.

		key := string(line[:eqTok])
		value := string(line[eqTok+1 : valEndIdx])

		config[key] = value
	}

	return config, nil
}

func setupCvmfs(o *Opts) error {
	if o.HasAlienCache {
		// Make sure the volume is writable by CVMFS processes.
		if err := os.Chmod(AlienCachePath, 0777); err != nil {
			return err
		}
	}

	// Clean up local cache. It may be dirty after previous nodeplugin Pod runs.

	log.Debugf("Cleaning up local cache directory %s...", LocalCachePath)

	cvmfsConfig, err := readEffectiveDefaultCvmfsConfig()
	if err != nil {
		return fmt.Errorf("failed to read CVMFS config: %v", err)
	}

	cacheDir := cvmfsConfig["CVMFS_CACHE_BASE"]
	if cacheDir == "" {
		cacheDir = LocalCachePath
	}

	if err := removeDirContents(cacheDir); err != nil {
		return fmt.Errorf("failed to clean up local cache directory %s: %v", cacheDir, err)
	}

	log.Debugf("Finished cleaning up local cache directory %s", cacheDir)

	// Set up configuration required for autofs with CVMFS to work properly.

	if _, err := exec.CombinedOutput(goexec.Command("cvmfs_config", "setup", "nocfgmod", "nostart", "noautofs")); err != nil {
		return fmt.Errorf("failed to setup CVMFS config: %v", err)
	}

	return nil
}

func setupAutofs(o *Opts) error {
	writeFmtFile := func(filepath, format string, fmtValues ...any) error {
		if err := os.WriteFile(filepath, []byte(fmt.Sprintf(format, fmtValues...)), 0644); err != nil {
			return fmt.Errorf("failed to write autofs configuration to %s: %v", filepath, err)
		}
		return nil
	}

	if err := writeFmtFile(
		"/etc/autofs.conf",
		`# Generated by automount-runner for CVMFS CSI.
[ autofs ]
timeout = %d
browse_mode = no
`,
		o.UnmountTimeoutSeconds,
	); err != nil {
		return err
	}

	if err := writeFmtFile(
		"/etc/auto.master",
		`# Generated by automount-runner for CVMFS CSI.
/cvmfs /etc/auto.cvmfs
`,
	); err != nil {
		return err
	}

	return nil
}

func Init(o *Opts) error {
	ver, err := cvmfsVersion()
	if err != nil {
		return err
	}

	log.Infof("%s", ver)

	if err := setupCvmfs(o); err != nil {
		return err
	}

	if err := setupAutofs(o); err != nil {
		return err
	}

	return nil
}

func RunBlocking() error {
	args := []string{
		"--foreground",
	}

	if log.LevelEnabled(log.LevelDebug) {
		args = append(args, "--verbose")

		// Log info about autofs mount in /cvmfs.

		isAutofs, err := IsAutofs("/cvmfs")
		if err != nil {
			log.Fatalf("Failed to stat /cvmfs: %v", err)
		}

		if isAutofs {
			log.Debugf("autofs already mounted in /cvmfs, automount daemon will reconnect...")
		} else {
			log.Debugf("autofs not mounted in /cvmfs, automount daemon will mount it now...")
		}
	}

	if log.LevelEnabled(log.LevelTrace) {
		// automount passes -O options to the underlying fs mounts.
		// Enable CVMFS debug logging.
		args = append(args, "-O", "debug", "--debug")
	}

	cmd := goexec.Command("automount", args...)

	// Set-up piping output for stdout and stderr to driver's logging.

	outp, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	cmd.Stderr = cmd.Stdout

	// Run automount.

	scanner := bufio.NewScanner(outp)
	scanner.Split(bufio.ScanLines)

	go func() {
		for scanner.Scan() {
			log.Infof("automount[%d]: %s", cmd.Process.Pid, scanner.Text())
		}
	}()

	// Catch SIGTERM and SIGKILL and forward it to the automount process.

	autofsTryCleanAtExit := env.GetAutofsTryCleanAtExit()

	sigCh := make(chan os.Signal, 2)
	defer close(sigCh)

	var exitedWithSigTerm atomic.Bool

	go func() {
		for {
			sig, more := <-sigCh
			if !more {
				break
			}

			if !autofsTryCleanAtExit && sig == syscall.SIGTERM {
				// automount daemon unmounts the autofs root in /cvmfs upon
				// receiving SIGTERM. This makes it impossible to reconnect
				// the daemon to the mount later, so all consumer Pods will
				// loose their mounts CVMFS, without the possibility of restoring
				// them (unless these Pods are restarted too). The implication
				// is that the nodeplugin is just being restarted, and will be
				// needed again.
				//
				// SIGKILL is handled differently in automount, as this forces
				// the daemon to skip the cleanup at exit, leaving the autofs
				// mount behind and making it possible to reconnect to it later.
				// We make a use of this, and unless the admin doesn't explicitly
				// ask for cleanup with AUTOFS_TRY_CLEAN_AT_EXIT env var, no cleanup
				// is done.
				//
				// Also, we intentionally don't unmount the existing autofs-managed
				// mounts inside /cvmfs, so that any existing consumers receive ENOTCONN
				// (due to broken FUSE mounts), so that accidental `mkdir -p` won't
				// succeed. They are cleaned by the daemon on startup.
				//
				// TODO: remove this once the automount daemon supports skipping
				//       cleanup (via a command line flag).

				log.Debugf("Sending SIGKILL to automount daemon")

				exitedWithSigTerm.Store(true)
				cmd.Process.Signal(syscall.SIGKILL)
				break
			}

			cmd.Process.Signal(sig)
		}
	}()

	shutdownSignals := []os.Signal{
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGKILL,
	}

	signal.Notify(sigCh, shutdownSignals...)

	// Start automount daemon.

	log.Infof("Starting automount daemon prog=%s args=%v", cmd.Path, cmd.Args)
	if err := cmd.Start(); err != nil {
		return err
	}
	log.Infof("Started automount daemon PID %d", cmd.Process.Pid)

	// Wait until automount exits.

	cmd.Wait()

	if !exitedWithSigTerm.Load() && cmd.ProcessState.ExitCode() != 0 {
		log.Fatalf(fmt.Sprintf("automount[%d] has exited unexpectedly: %s", cmd.Process.Pid, cmd.ProcessState))
	}

	log.Infof("automount[%d] has exited: %s", cmd.Process.Pid, cmd.ProcessState)

	return nil
}

func IsAutofs(path string) (bool, error) {
	const fsType = 0x187

	statfs := syscall.Statfs_t{}
	err := syscall.Statfs(path, &statfs)
	if err != nil {
		return false, err
	}

	return statfs.Type == fsType, nil
}
