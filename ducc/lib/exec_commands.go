package lib

import (
	"bytes"
	"fmt"
	"io"
	"os/exec"

	log "github.com/sirupsen/logrus"
)

type execCmd struct {
	cmd   *exec.Cmd
	err   io.ReadCloser
	out   io.ReadCloser
	in    io.WriteCloser
	stdin *io.ReadCloser
}

func ExecCommand(input ...string) *execCmd {
	Log().WithFields(log.Fields{"action": "executing"}).Info(input)
	cmd := exec.Command(input[0], input[1:]...)
	stdout, errOUT := cmd.StdoutPipe()
	if errOUT != nil {
		LogE(errOUT).Warning("Impossible to obtain the STDOUT pipe")
		return nil
	}
	stderr, errERR := cmd.StderrPipe()
	if errERR != nil {
		LogE(errERR).Warning("Impossible to obtain the STDERR pipe")
		return nil
	}
	stdin, errERR := cmd.StdinPipe()
	if errERR != nil {
		LogE(errERR).Warning("Impossible to obtain the STDIN pipe")
		return nil
	}

	return &execCmd{cmd: cmd, err: stderr, out: stdout, in: stdin}
}

func (e *execCmd) StdIn(input io.ReadCloser) *execCmd {
	if e == nil {
		err := fmt.Errorf("Use of nil execCmd")
		LogE(err).Error("Call StdIn with nil cmd, maybe error in the constructor")
		return nil
	}

	e.stdin = &input

	return e
}

func (e *execCmd) StdOut() io.ReadCloser {
	if e == nil {
		LogE(fmt.Errorf("No cmd structure passed as input.")).Error("Impossible to get the stdout.")
		return nil
	}
	return e.out
}

func (e *execCmd) StartWithOutput() (error, bytes.Buffer, bytes.Buffer) {
	var outb, errb bytes.Buffer
	e.cmd.Stdout = &outb
	e.cmd.Stderr = &errb

	if e == nil {
		err := fmt.Errorf("Use of nil execCmd")
		LogE(err).Error("Call start with nil cmd, maybe error in the constructor")
		return err, outb, errb
	}

	err := e.cmd.Start()
	if err != nil {
		LogE(err).Error("Error in starting the command")
		return err, outb, errb
	}

	if e.stdin != nil {
		go func() {
			defer (*e.stdin).Close()
			defer e.in.Close()
			n, err := io.Copy(e.in, *e.stdin)
			Log().WithFields(log.Fields{"n": n}).Info("Copied n bytes to STDIN")
			if err != nil {
				LogE(err).Error("Error in copying the input into STDIN.")
				return
			}
			for n > 0 {
				n, err = io.Copy(e.in, *e.stdin)
				if err != nil {
					LogE(err).Error("Error in copying the input into STDIN")
					return
				}
				Log().WithFields(log.Fields{"n": n}).Info("Copied additionally n bytes to STDIN")
			}
		}()
	}
	err = e.cmd.Wait()
	return err, outb, errb
}

func (e *execCmd) Start() error {

	err, stdout, stderr := e.StartWithOutput()
	if err == nil {
		return nil
	} else {
		LogE(err).Error("Error in executing the command")
		Log().WithFields(log.Fields{"pipe": "STDOUT"}).Info(string(stdout.Bytes()))
		Log().WithFields(log.Fields{"pipe": "STDERR"}).Info(string(stderr.Bytes()))
		return err
	}
	return nil
}

func (e *execCmd) Env(key, value string) *execCmd {
	if e == nil {
		err := fmt.Errorf("Use of nil execCmd")
		LogE(err).Error("Set ENV to nil cmd, maybe error in the constructor")
		return nil
	}
	e.cmd.Env = append(e.cmd.Env, fmt.Sprintf("%s=%s", key, value))
	return e
}
