package receiver

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"

	gw "github.com/cvmfs/gateway/internal/gateway"
	stats "github.com/cvmfs/gateway/internal/gateway/statistics"
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
	receiverError     // Unused
	receiverTestCrash // Used only in testing
)

// Receiver contains the operations that "receiver" worker processes perform
type Receiver interface {
	Quit() error
	Echo() error
	SubmitPayload(leasePath string, payload io.Reader, digest string, headerSize int) error
	Commit(leasePath, oldRootHash, newRootHash string, tag gw.RepositoryTag) (uint64, error)
	Interrupt() error // like Ctrl-C SIGTERM -2
	// Kill() error // like Crtl-D SIGKILL -9
	TestCrash() error
}

type ReceiverReply struct {
	Status        string           `json:"status"`
	Reason        string           `json:"reason"`
	FinalRevision uint64           `json:"final_revision"`
	Statistics    stats.Statistics `json:"statistics"`
}

// NewReceiver is the factory method for Receiver types
func NewReceiver(ctx context.Context, execPath string, mock bool, statsMgr *stats.StatisticsMgr, args ...string) (Receiver, error) {
	if mock {
		return NewMockReceiver(ctx)
	}

	return NewCvmfsReceiver(ctx, execPath, statsMgr, args...)
}

// CvmfsReceiver spawns an external cvmfs_receiver worker process
type CvmfsReceiver struct {
	worker       *exec.Cmd
	workerCmdIn  io.WriteCloser
	workerCmdOut io.ReadCloser
	workerStderr io.ReadCloser
	workerStdout io.ReadCloser
	ctx          context.Context
	statsMgr     *stats.StatisticsMgr
}

// NewCvmfsReceiver will spawn an external cvmfs_receiver worker process and wait for a command
func NewCvmfsReceiver(ctx context.Context, execPath string, statsMgr *stats.StatisticsMgr, args ...string) (*CvmfsReceiver, error) {
	if _, err := os.Stat(execPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("worker process executable not found: %w", err)
	}

	debugLog := "/var/log/cvmfs_receiver/debug.log"
	cmdLine := []string{"-i", "3", "-o", "4", "-d", debugLog}
	cmdLine = append(cmdLine, args...)
	cmd := exec.Command(execPath, cmdLine...)

	workerInRead, workerInWrite, err := os.Pipe()
	if err != nil {
		return nil, fmt.Errorf("could not create worker input pipe: %w", err)
	}
	workerOutRead, workerOutWrite, err := os.Pipe()
	if err != nil {
		return nil, fmt.Errorf("could not create worker output pipe: %w", err)
	}

	// it is necessary to close this two files, otherwise, if the receiver crash,
	// a read on the `workerOutRead` / `workerCmdOut` will hang forever.
	// details: https://web.archive.org/web/20200429092830/https://redbeardlab.com/2020/04/29/on-linux-pipes-fork-and-passing-file-descriptors-to-other-process/
	// Note how we close them **after** the cmd.Start call finish, using defer.
	// Indeed cmd.Start copies the file descriptor in the new process space,
	// closing them before it would be an error, because the new process would
	// get closed filed descriptor.
	defer workerInRead.Close()
	defer workerOutWrite.Close()

	cmd.ExtraFiles = []*os.File{workerInRead, workerOutWrite}

	stderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, fmt.Errorf("could not create worker stderr pipe: %w", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("could not create worker stdout pipe: %w", err)
	}

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("could not start worker process: %w", err)
	}

	gw.LogC(ctx, "receiver", gw.LogDebug).
		Str("command", "start").
		Msg("worker process ready")

	return &CvmfsReceiver{
		worker: cmd, workerCmdIn: workerInWrite, workerCmdOut: workerOutRead,
		workerStderr: stderr, workerStdout: stdout, ctx: ctx, statsMgr: statsMgr}, nil
}

// Quit command is sent to the worker
func (r *CvmfsReceiver) Quit() error {
	needToWait := true
	defer func() {
		r.workerCmdIn.Close()
		r.workerCmdOut.Close()
		if needToWait {
			r.worker.Wait()
		}
	}()

	if _, err := r.call(receiverQuit, []byte{}, nil); err != nil {
		return fmt.Errorf("worker 'quit' call failed: %w", err)
	}

	var buf1 []byte
	if _, err := io.ReadFull(r.workerStderr, buf1); err != nil {
		return fmt.Errorf("could not retrieve worker stderr: %w", err)
	}
	gw.LogC(r.ctx, "receiver", gw.LogDebug).
		Str("pipe", "stderr").
		Msg(string(buf1))

	var buf2 []byte
	if _, err := io.ReadFull(r.workerStdout, buf2); err != nil {
		return fmt.Errorf("could not retrieve worker stdout: %w", err)
	}
	gw.LogC(r.ctx, "receiver", gw.LogDebug).
		Str("pipe", "stdout").
		Msg(string(buf2))

	err := r.worker.Wait()
	needToWait = false
	if err != nil {
		return fmt.Errorf("waiting for worker process failed: %w", err)
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
		return fmt.Errorf("worker 'echo' call failed: %w", err)
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
		return fmt.Errorf("request encoding failed: %w", err)
	}
	reply, err := r.call(receiverSubmitPayload, buf, payload)
	if err != nil {
		return fmt.Errorf("worker 'payload submission' call failed: %w", err)
	}

	parsedReply, result := parseReceiverReply(reply)

	gw.LogC(r.ctx, "receiver", gw.LogDebug).
		Str("command", "submit payload").
		Str("lease_path", leasePath).
		Msgf("result: %v", result)

	if result == nil {
		r.statsMgr.MergeIntoLeaseStatistics(leasePath, &parsedReply.Statistics)
	}

	return result
}

// Commit command is sent to the worker
func (r *CvmfsReceiver) Commit(leasePath, oldRootHash, newRootHash string, tag gw.RepositoryTag) (uint64, error) {
	stats, err := r.statsMgr.PopLease(leasePath)
	if err != nil {
		return 0, fmt.Errorf("could not obtain statistics counters: %w", err)
	}
	req := map[string]interface{}{
		"lease_path":      leasePath,
		"old_root_hash":   oldRootHash,
		"new_root_hash":   newRootHash,
		"tag_name":        tag.Name,
		"tag_description": tag.Description,
		"statistics":      stats,
	}
	buf, err := json.Marshal(&req)
	if err != nil {
		return 0, fmt.Errorf("request encoding failed: %w", err)
	}

	reply, err := r.call(receiverCommit, buf, nil)
	if err != nil {
		return 0, fmt.Errorf("worker 'commit' call failed: %w", err)
	}

	parsedReply, result := parseReceiverReply(reply)

	gw.LogC(r.ctx, "receiver", gw.LogDebug).
		Str("command", "commit").
		Str("lease_path", leasePath).
		Msgf("result: %v", result)

	return parsedReply.FinalRevision, result
}

func (r *CvmfsReceiver) Interrupt() error {
	err := r.worker.Process.Signal(os.Interrupt)
	gw.LogC(r.ctx, "receiver", gw.LogDebug).
		Str("command", "interrupt").
		Msgf("result (err): %v", err)
	return err
}

// Method used only in testing, we provide an empty implementation here
func (r *CvmfsReceiver) TestCrash() error {
	reply, err := r.call(receiverTestCrash, nil, nil)
	if err != nil {
		return err
	}
	_, result := parseReceiverReply(reply)
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
		return fmt.Errorf("could not write request: %w", err)
	}
	if payload != nil {
		if _, err := io.Copy(r.workerCmdIn, payload); err != nil {
			r.Interrupt()
			return fmt.Errorf("could not write request payload: %w", err)
		}
	}
	return nil
}

func (r *CvmfsReceiver) reply() ([]byte, error) {
	buf := make([]byte, 4)
	if _, err := io.ReadFull(r.workerCmdOut, buf); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, fmt.Errorf("possible that the receiver crashed: %w", err)
		}
		return nil, fmt.Errorf("could not read reply size: %w", err)
	}
	repSize := int32(binary.LittleEndian.Uint32(buf))

	reply := make([]byte, repSize)
	if _, err := io.ReadFull(r.workerCmdOut, reply); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, fmt.Errorf("possible that the receiver crashed: %w", err)
		}
		return nil, fmt.Errorf("could not read reply body: %w", err)
	}

	return reply, nil
}

func parseReceiverReply(reply []byte) (*ReceiverReply, error) {
	res := &ReceiverReply{}
	if err := json.Unmarshal(reply, res); err != nil {
		return nil, fmt.Errorf("could not decode receiver reply '"+string(reply[:])+"': %w", err)
	}
	if res.Status != "ok" {
		return res, Error(res.Reason)
	}
	return res, nil
}
