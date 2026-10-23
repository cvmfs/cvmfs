package receiver

import (
	"context"
	"fmt"
	"io"
	"time"

	gw "github.com/cvmfs/gateway/internal/gateway"
)

// MockReceiver is a mocked implementation of the Receiver interface, for testing
// Can implement fault injection
type MockReceiver struct {
	ctx context.Context
}

// mockCommitGate, when non-nil, blocks MockReceiver.Commit until it can receive
// from the channel. mockCommitEntered is signalled each time a commit reaches
// the gate. Both are used by tests to keep a commit parked in flight.
var (
	mockCommitGate    chan struct{}
	mockCommitEntered chan struct{}
)

// SetMockCommitGate makes every subsequent mock commit block until a value can
// be received from release. The returned channel receives a value each time a
// commit reaches the gate, letting tests wait until a commit is parked in
// flight. Pass nil to remove the gate.
func SetMockCommitGate(release chan struct{}) <-chan struct{} {
	mockCommitGate = release
	if release == nil {
		mockCommitEntered = nil
		return nil
	}
	entered := make(chan struct{}, 1)
	mockCommitEntered = entered
	return entered
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

func (r *MockReceiver) Commit(leasePath, oldRootHash, newRootHash string, tag gw.RepositoryTag, leaseExpiration time.Time) (uint64, error) {
	if entered := mockCommitEntered; entered != nil {
		select {
		case entered <- struct{}{}:
		default:
		}
	}
	if gate := mockCommitGate; gate != nil {
		<-gate
	}
	// Mirror the real receiver: refuse to publish once the commit deadline (the
	// lease expiration minus the gateway's configured margin) has passed.
	if !leaseExpiration.IsZero() && time.Now().After(leaseExpiration) {
		gw.LogC(r.ctx, "mock_receiver", gw.LogDebug).
			Str("command", "commit").
			Str("lease_path", leasePath).
			Msgf("lease expired during commit")
		return 0, Error("lease_expired")
	}
	gw.LogC(r.ctx, "mock_receiver", gw.LogDebug).
		Str("command", "commit").
		Str("lease_path", leasePath).
		Msgf("new revision committed")
	return 1, nil
}

func (r *MockReceiver) Graft(leasePath, oldRootHash, newRootHash string, tag gw.RepositoryTag, leaseExpiration time.Time) (uint64, error) {
	// Mirror the real receiver: refuse to publish once the commit deadline (the
	// lease expiration minus the gateway's configured margin) has passed.
	if !leaseExpiration.IsZero() && time.Now().After(leaseExpiration) {
		gw.LogC(r.ctx, "mock_receiver", gw.LogDebug).
			Str("command", "graft").
			Str("lease_path", leasePath).
			Msgf("lease expired during commit")
		return 0, Error("lease_expired")
	}
	gw.LogC(r.ctx, "mock_receiver", gw.LogDebug).
		Str("command", "graft").
		Str("lease_path", leasePath).
		Msgf("new revision grafted")
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
