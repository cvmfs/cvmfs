package receiver

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"

	gw "github.com/cvmfs/gateway/internal/gateway"
	"github.com/pkg/errors"
)

// Error is returned by the various receiver commands in case of error
type Error string

func (e Error) Error() string {
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

// Receiver contains the operations that "receiver" worker processes perform
type Receiver interface {
	Quit() error
	Echo() error
	SubmitPayload(leasePath string, payload io.Reader, digest string, headerSize int) error
	Commit(leasePath, oldRootHash, newRootHash string, tag gw.RepositoryTag) error
}

// NewReceiver is the factory method for Receiver types
func NewReceiver(ctx context.Context, execPath string, mock bool) (Receiver, error) {
	if mock {
		return NewMockReceiver(ctx)
	}

	return NewCvmfsReceiver(ctx, execPath)
}

// CvmfsReceiver spawns an external cvmfs_receiver worker process
type CvmfsReceiver struct {
	worker       *exec.Cmd
	workerCmdIn  io.WriteCloser
	workerCmdOut io.ReadCloser
	workerStderr io.ReadCloser
	workerStdout io.ReadCloser
	ctx          context.Context
}

type ReceiverReply struct {
	Status string `json:"status"`
	Reason string `json:"reason"`
}

// NewCvmfsReceiver will spawn an external cvmfs_receiver worker process and wait for a command
func NewCvmfsReceiver(ctx context.Context, execPath string) (*CvmfsReceiver, error) {
	if _, err := os.Stat(execPath); os.IsNotExist(err) {
		return nil, errors.Wrap(err, "worker process executable not found")
	}

	cmd := exec.Command(execPath, "-i", strconv.Itoa(3), "-o", strconv.Itoa(4))

	workerInRead, workerInWrite, err := os.Pipe()
	if err != nil {
		return nil, errors.Wrap(err, "could not create worker input pipe")
	}
	workerOutRead, workerOutWrite, err := os.Pipe()
	if err != nil {
		return nil, errors.Wrap(err, "could not create worker output pipe")
	}

	cmd.ExtraFiles = []*os.File{workerInRead, workerOutWrite}

	stderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, errors.Wrap(err, "could not create worker stderr pipe")
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, errors.Wrap(err, "could not create worker stdout pipe")
	}

	if err := cmd.Start(); err != nil {
		return nil, errors.Wrap(err, "could not start worker process")
	}

	// it is necessary to close this two files, otherwise, if the receiver crash,
	// a read on the `workerOutRead` / `workerCmdOut` will hang forever.
	// details: https://web.archive.org/web/20200429092830/https://redbeardlab.com/2020/04/29/on-linux-pipes-fork-and-passing-file-descriptors-to-other-process/
	workerInRead.Close()
	workerOutWrite.Close()

	gw.LogC(ctx, "receiver", gw.LogDebug).
		Str("command", "start").
		Msg("worker process ready")

	return &CvmfsReceiver{
		worker: cmd, workerCmdIn: workerInWrite, workerCmdOut: workerOutRead,
		workerStderr: stderr, workerStdout: stdout, ctx: ctx}, nil
}

// Quit command is sent to the worker
func (r *CvmfsReceiver) Quit() error {
	defer func() {
		r.workerCmdIn.Close()
		r.workerCmdOut.Close()
	}()

	if _, err := r.call(receiverQuit, []byte{}, nil); err != nil {
		return errors.Wrap(err, "worker 'quit' call failed")
	}

	var buf1 []byte
	if _, err := io.ReadFull(r.workerStderr, buf1); err != nil {
		return errors.Wrap(err, "could not retrieve worker stderr")
	}
	gw.LogC(r.ctx, "receiver", gw.LogDebug).
		Str("pipe", "stderr").
		Msg(string(buf1))

	var buf2 []byte
	if _, err := io.ReadFull(r.workerStdout, buf2); err != nil {
		return errors.Wrap(err, "could not retrieve worker stdout")
	}
	gw.LogC(r.ctx, "receiver", gw.LogDebug).
		Str("pipe", "stdout").
		Msg(string(buf2))

	if err := r.worker.Wait(); err != nil {
		return errors.Wrap(err, "waiting for worker process failed")
	}

	gw.LogC(r.ctx, "receiver", gw.LogDebug).
		Str("command", "quit").
		Msg("worker process has stopped")

	return nil
}

// Echo command is sent to the worker
func (r *CvmfsReceiver) Echo() error {
	rep, err := r.call(receiverEcho, []byte("Ping"), nil)
	if err != nil {
		return errors.Wrap(err, "worker 'echo' call failed")
	}
	reply := string(rep)

	if !strings.HasPrefix(reply, "PID: ") {
		return fmt.Errorf("invalid 'echo' reply received: %v", reply)
	}

	gw.LogC(r.ctx, "receiver", gw.LogDebug).
		Str("command", "echo").
		Msgf("reply: %v", reply)

	return nil
}

// SubmitPayload command is sent to the worker
func (r *CvmfsReceiver) SubmitPayload(leasePath string, payload io.Reader, digest string, headerSize int) error {
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

	gw.LogC(r.ctx, "receiver", gw.LogDebug).
		Str("command", "submit payload").
		Msgf("result: %v", result)

	return result
}

// Commit command is sent to the worker
func (r *CvmfsReceiver) Commit(leasePath, oldRootHash, newRootHash string, tag gw.RepositoryTag) error {
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

	reply, err := r.call(receiverCommit, buf, nil)
	if err != nil {
		return errors.Wrap(err, "worker 'commit' call failed")
	}

	result := toReceiverError(reply)

	gw.LogC(r.ctx, "receiver", gw.LogDebug).
		Str("command", "commit").
		Msgf("result: %v", result)

	return result
}

func (r *CvmfsReceiver) call(reqID receiverOp, msg []byte, payload io.Reader) ([]byte, error) {
	if err := r.request(reqID, msg, payload); err != nil {
		return nil, err
	}
	return r.reply()
}

func (r *CvmfsReceiver) request(reqID receiverOp, msg []byte, payload io.Reader) error {
	buf := make([]byte, 8+len(msg))
	binary.LittleEndian.PutUint32(buf[:4], uint32(reqID))
	binary.LittleEndian.PutUint32(buf[4:8], uint32(len(msg)))
	copy(buf[8:], msg)

	if _, err := r.workerCmdIn.Write(buf); err != nil {
		return errors.Wrap(err, "could not write request")
	}
	if payload != nil {
		if _, err := io.Copy(r.workerCmdIn, payload); err != nil {
			return errors.Wrap(err, "could not write request payload")
		}
	}
	return nil
}

func (r *CvmfsReceiver) reply() ([]byte, error) {
	buf := make([]byte, 4)
	if _, err := io.ReadFull(r.workerCmdOut, buf); err != nil {
		if (err == io.EOF) || (err == io.ErrUnexpectedEOF) {
			return nil, errors.Wrap(err, "possible that the receiver crashed")
		}
		return nil, errors.Wrap(err, "could not read reply size")
	}
	repSize := int32(binary.LittleEndian.Uint32(buf))

	reply := make([]byte, repSize)
	if _, err := io.ReadFull(r.workerCmdOut, reply); err != nil {
		if (err == io.EOF) || (err == io.ErrUnexpectedEOF) {
			return nil, errors.Wrap(err, "possible that the receiver crashed")
		}
		return nil, errors.Wrap(err, "could not read reply body")
	}

	return reply, nil
}

func toReceiverError(reply []byte) error {
	res := ReceiverReply{}
	if err := json.Unmarshal(reply, &res); err != nil {
		return errors.Wrap(err, "could not decode reply")
	}

	if res.Status == "ok" {
		return nil
	}

	if res.Reason != "" {
		return Error(res.Reason)
	}

	return fmt.Errorf("invalid reply")
}
