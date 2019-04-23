package backend

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"

	gw "github.com/cvmfs/gateway/internal/gateway"
	"github.com/pkg/errors"
)

// ReceiverError is returned by the various receiver commands in case of error
type ReceiverError string

func (e ReceiverError) Error() string {
	return string(e)
}

// receiverOp is used to identify the different operation performed
// by the cvmfs_receiver process
type receiverOp int32

// The different operations are defined as constants. The numbering
// must match (enum receiver::Request from "cvmfs.git/cvmfs/receiver/reactor.h")
const (
	receiverQuit receiverOp = iota
	receiverEcho
	receiverGenerateToken // Unused
	receiverGetTokenID    // Unused
	receiverCheckToken    // Unused
	receiverSubmitPayload
	receiverCommit
	receiverError // Unused
)

// Receiver provides an interface to the external cvmfs_receiver worker process
type Receiver struct {
	worker *exec.Cmd
	stdin  io.Writer
	stdout io.Reader
}

// NewReceiver will spawn an external cvmfs_receiver worker process and wait for a command
func NewReceiver(execPath string) (*Receiver, error) {
	if _, err := os.Stat(execPath); os.IsNotExist(err) {
		return nil, errors.Wrap(err, "worker process executable not found")
	}

	cmd := exec.Command(execPath)

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, errors.Wrap(err, "could not get writable pipe")
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, errors.Wrap(err, "could not get readable pipe")
	}

	if err := cmd.Start(); err != nil {
		return nil, errors.Wrap(err, "could not start worker process")
	}

	gw.Log.Debug().
		Str("component", "receiver").
		Str("command", "start").
		Msgf("worker process ready")

	return &Receiver{worker: cmd, stdin: stdin, stdout: stdout}, nil
}

// Quit command is sent to the worker
func (r *Receiver) Quit() error {
	if _, err := r.call(receiverQuit, []byte{}, []byte{}); err != nil {
		return errors.Wrap(err, "worker 'quit' call failed")
	}

	if err := r.worker.Wait(); err != nil {
		return errors.Wrap(err, "waiting for worker process failed")
	}

	gw.Log.Debug().
		Str("component", "receiver").
		Str("command", "quit").
		Msgf("worker process has stopped")

	return nil
}

// Echo command is sent to the worker
func (r *Receiver) Echo() error {
	rep, err := r.call(receiverEcho, []byte("Ping"), []byte{})
	if err != nil {
		return errors.Wrap(err, "worker 'echo' call failed")
	}
	reply := string(rep)

	if !strings.HasPrefix(reply, "PID: ") {
		return fmt.Errorf("invalid 'echo' reply received: %v", reply)
	}

	gw.Log.Debug().
		Str("component", "receiver").
		Str("command", "echo").
		Msgf("reply: %v", reply)

	return nil
}

// SubmitPayload command is sent to the worker
func (r *Receiver) SubmitPayload(leasePath string, payload []byte, digest string, headerSize int) error {
	req := map[string]interface{}{"path": leasePath, "digest": digest, "header_size": headerSize}
	buf, err := json.Marshal(&req)
	if err != nil {
		return errors.Wrap(err, "request encoding failed")
	}
	reply, err := r.call(receiverSubmitPayload, buf, payload)
	if err != nil {
		return errors.Wrap(err, "worker 'payload submission' call failed")
	}

	result := toReceiverError(reply)

	gw.Log.Debug().
		Str("component", "receiver").
		Str("command", "submit payload").
		Msgf("result: %v", result)

	return result
}

// Commit command is sent to the worker
func (r *Receiver) Commit(leasePath, oldRootHash, newRootHash string, tag RepositoryTag) error {
	req := map[string]interface{}{
		"lease_path":      leasePath,
		"old_root_hash":   oldRootHash,
		"new_root_hash":   newRootHash,
		"tag_name":        tag.Name,
		"tag_channel":     tag.Channel,
		"tag_description": tag.Description,
	}
	buf, err := json.Marshal(&req)
	if err != nil {
		return errors.Wrap(err, "request encoding failed")
	}

	reply, err := r.call(receiverCommit, buf, []byte{})
	if err != nil {
		return errors.Wrap(err, "worker 'commit' call failed")
	}

	result := toReceiverError(reply)

	gw.Log.Debug().
		Str("component", "receiver").
		Str("command", "commit").
		Msgf("result: %v", result)

	return result
}

func (r *Receiver) call(reqID receiverOp, msg, payload []byte) ([]byte, error) {
	if err := r.request(reqID, msg, payload); err != nil {
		return nil, err
	}
	return r.reply()
}

func (r *Receiver) request(reqID receiverOp, msg, payload []byte) error {
	if err := binary.Write(r.stdin, binary.LittleEndian, reqID); err != nil {
		return errors.Wrap(err, "could not write request id")
	}
	if err := binary.Write(r.stdin, binary.LittleEndian, int32(len(msg))); err != nil {
		return errors.Wrap(err, "could not write request size")
	}
	if _, err := r.stdin.Write(msg); err != nil {
		return errors.Wrap(err, "could not write request body")
	}
	if payload != nil {
		if _, err := r.stdin.Write(payload); err != nil {
			return errors.Wrap(err, "could not write request payload")
		}
	}
	return nil
}

func (r *Receiver) reply() ([]byte, error) {
	var repSize int32
	if err := binary.Read(r.stdout, binary.LittleEndian, &repSize); err != nil {
		return nil, errors.Wrap(err, "could not read reply size")
	}

	reply := make([]byte, repSize)
	if _, err := r.stdout.Read(reply); err != nil {
		return nil, errors.Wrap(err, "could not read reply body")
	}

	return reply, nil
}

func toReceiverError(reply []byte) error {
	res := make(map[string]string)
	if err := json.Unmarshal(reply, &res); err != nil {
		return errors.Wrap(err, "could not decode reply")
	}

	if status, ok := res["status"]; ok {
		if status == "ok" {
			return nil
		}

		if reason, ok := res["reason"]; ok {
			return ReceiverError(reason)
		}

		return fmt.Errorf("invalid reply")
	}

	return fmt.Errorf("invalid reply")
}
