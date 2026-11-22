package exec

import (
	"bytes"
	"fmt"
	"io"
	"os/exec"
	"strings"

	l "github.com/cvmfs/ducc/log"
	log "github.com/sirupsen/logrus"
)

type execCmd struct {
	cmd   *exec.Cmd
	err   io.ReadCloser
	out   io.ReadCloser
	in    io.WriteCloser
	stdin *io.ReadCloser
	log   *log.Entry
}

func ExecCommand(input ...string) *execCmd {
	return ExecCommandWithLogger(nil, input...)
}

func ExecCommandWithLogger(logger *log.Entry, input ...string) *execCmd {
	logger = l.Ensure(logger)
	logger.WithFields(log.Fields{"action": "executing", "command": input}).Trace("Running command")
	cmd := exec.Command(input[0], input[1:]...)
	stdout, errOUT := cmd.StdoutPipe()
	if errOUT != nil {
		logger.WithField("error", errOUT).Warning("Impossible to obtain the STDOUT pipe")
		return nil
	}
	stderr, errERR := cmd.StderrPipe()
	if errERR != nil {
		logger.WithField("error", errERR).Warning("Impossible to obtain the STDERR pipe")
		return nil
	}
	stdin, errERR := cmd.StdinPipe()
	if errERR != nil {
		logger.WithField("error", errERR).Warning("Impossible to obtain the STDIN pipe")
		return nil
	}

	return &execCmd{cmd: cmd, err: stderr, out: stdout, in: stdin, log: logger}
}

func (e *execCmd) StdIn(input io.ReadCloser) *execCmd {
	if e == nil {
		err := fmt.Errorf("Use of nil execCmd")
		l.LogE(err).Error("Call StdIn with nil cmd, maybe error in the constructor")
		return nil
	}

	e.stdin = &input

	return e
}

func (e *execCmd) StdOut() io.ReadCloser {
	if e == nil {
		l.LogE(fmt.Errorf("No cmd structure passed as input.")).Error("Impossible to get the stdout.")
		return nil
	}
	return e.out
}

func (e *execCmd) StartWithOutput() (error, bytes.Buffer, bytes.Buffer) {
	if e == nil {
		err := fmt.Errorf("Use of nil execCmd")
		l.LogE(err).Error("Call start with nil cmd, maybe error in the constructor")
		return err, bytes.Buffer{}, bytes.Buffer{}
	}

	var outb, errb bytes.Buffer
	e.cmd.Stdout = &outb
	e.cmd.Stderr = &errb
	logger := l.Ensure(e.log)

	err := e.cmd.Start()
	if err != nil {
		logger.WithField("error", err).Error("Error in starting the command")
		return err, outb, errb
	}

	if e.stdin != nil {
		go func() {
			defer (*e.stdin).Close()
			defer e.in.Close()
			n, err := io.Copy(e.in, *e.stdin)
			logger.WithFields(log.Fields{"n": n}).Trace("Copied n bytes to STDIN")
			if err != nil {
				logger.WithField("error", err).Error("Error in copying the input into STDIN.")
				return
			}
			for n > 0 {
				n, err = io.Copy(e.in, *e.stdin)
				if err != nil {
					logger.WithField("error", err).Error("Error in copying the input into STDIN")
					return
				}
				logger.WithFields(log.Fields{"n": n}).Trace("Copied additionally n bytes to STDIN")
			}
		}()
	}
	err = e.cmd.Wait()
	return err, outb, errb
}

func (e *execCmd) Start() error {
	err, stdout, stderr := e.StartWithOutput()
	logger := l.Ensure(e.log)
	if err == nil {
		return nil
	} else {
		logger.WithField("error", err).Error("Error in executing the command")
		logger.WithFields(log.Fields{"pipe": "STDOUT"}).Trace(string(stdout.Bytes()))
		logger.WithFields(log.Fields{"pipe": "STDERR"}).Trace(string(stderr.Bytes()))
		if tail := outputTail(stderr.Bytes(), stdout.Bytes()); tail != "" {
			return fmt.Errorf("%w: %s", err, tail)
		}
		return err
	}
}

// outputTail returns the last meaningful lines of the command output, so
// errors say why cvmfs_server failed instead of only "exit status 1".
// Progress dots and the echoed command line are skipped.
func outputTail(outputs ...[]byte) string {
	const maxLines, maxBytes = 4, 1024
	var lines []string
	for _, out := range outputs {
		all := strings.Split(string(out), "\n")
		for i := len(all) - 1; i >= 0 && len(lines) < maxLines; i-- {
			line := strings.TrimSpace(all[i])
			if line == "" || strings.Trim(line, ".") == "" ||
				strings.HasPrefix(line, "Executed Command") || strings.HasPrefix(line, "cvmfs_swissknife ") {
				continue
			}
			lines = append([]string{line}, lines...)
		}
		if len(lines) > 0 {
			break
		}
	}
	tail := strings.Join(lines, " | ")
	if len(tail) > maxBytes {
		tail = "..." + tail[len(tail)-maxBytes:]
	}
	return tail
}

func (e *execCmd) Env(key, value string) *execCmd {
	if e == nil {
		err := fmt.Errorf("Use of nil execCmd")
		l.LogE(err).Error("Set ENV to nil cmd, maybe error in the constructor")
		return nil
	}
	e.cmd.Env = append(e.cmd.Env, fmt.Sprintf("%s=%s", key, value))
	return e
}
