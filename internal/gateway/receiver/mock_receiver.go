package receiver

import (
	"context"
	"io"

	gw "github.com/cvmfs/gateway/internal/gateway"
)

// MockReceiver is a mocked implementation of the Receiver interface, for testing
// Can implement fault injection
type MockReceiver struct {
}

// NewMockReceiver constructs a new MockReceiver object which implements the
// Receiver interface
func NewMockReceiver(ctx context.Context) (Receiver, error) {
	return &MockReceiver{}, nil
}

// Quit command
func (r *MockReceiver) Quit() error {
	return nil
}

// Echo command
func (r *MockReceiver) Echo() error {
	return nil
}

// SubmitPayload command
func (r *MockReceiver) SubmitPayload(leasePath string, payload io.Reader, digest string, headerSize int) error {
	return nil
}

// Commit command
func (r *MockReceiver) Commit(leasePath, oldRootHash, newRootHash string, tag gw.RepositoryTag) error {
	return nil
}
