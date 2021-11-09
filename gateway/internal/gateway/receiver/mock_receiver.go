package receiver

import (
	"context"
	"fmt"
	"io"

	gw "github.com/cvmfs/gateway/internal/gateway"
)

// MockReceiver is a mocked implementation of the Receiver interface, for testing
// Can implement fault injection
type MockReceiver struct {
	ctx context.Context
}

// NewMockReceiver constructs a new MockReceiver object which implements the
// Receiver interface
func NewMockReceiver(ctx context.Context) (Receiver, error) {
	return &MockReceiver{ctx}, nil
}

func (r *MockReceiver) Quit() error {
	gw.LogC(r.ctx, "mock_receiver", gw.LogDebug).
		Str("command", "quit").
		Msg("worker process has stopped")
	return nil
}

func (r *MockReceiver) Echo() error {
	gw.LogC(r.ctx, "mock_receiver", gw.LogDebug).
		Str("command", "echo").
		Msgf("reply: PID: 12345")
	return nil
}

func (r *MockReceiver) Commit(leasePath, oldRootHash, newRootHash string, tag gw.RepositoryTag) (uint64, error) {
	gw.LogC(r.ctx, "mock_receiver", gw.LogDebug).
		Str("command", "commit").
		Str("lease_path", leasePath).
		Msgf("new revision committed")
	return 1, nil
}

func (r *MockReceiver) SubmitPayload(leasePath string, payload io.Reader, digest string, headerSize int) error {
	gw.LogC(r.ctx, "mock_receiver", gw.LogDebug).
		Str("command", "submit payload").
		Str("lease_path", leasePath).
		Msgf("payload submitted")
	return nil
}

func (r *MockReceiver) Interrupt() error {
	return nil
}

func (r *MockReceiver) TestCrash() error {
	gw.LogC(r.ctx, "mock_receiver", gw.LogDebug).
		Str("command", "test crash").
		Msgf("worker process is crashing")
	return fmt.Errorf("mock receiver has crashed")
}
