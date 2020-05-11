package receiver

import (
	"context"
	"io"

	gw "github.com/cvmfs/gateway/internal/gateway"
)

// MockReceiver is a mocked implementation of the Receiver interface, for testing
// Can implement fault injection
type MockReceiver struct {
	CvmfsReceiver
}

// NewMockReceiver constructs a new MockReceiver object which implements the
// Receiver interface
func NewMockReceiver(ctx context.Context, execPath string, args ...string) (Receiver, error) {
	CvmfsReceiver, err := NewCvmfsReceiver(ctx, execPath, args...)
	return &MockReceiver{*CvmfsReceiver}, err
}

func (r *MockReceiver) Commit(leasePath, oldRootHash, newRootHash string, tag gw.RepositoryTag) error {
	gw.LogC(r.CvmfsReceiver.ctx, "mock receiver", gw.LogWarn).
		Str("leaserPath", leasePath).
		Msg("Requested commit agains a testing mock, shortcircuit success")
	return nil
}

func (r *MockReceiver) SubmitPayload(leasePath string, payload io.Reader, digest string, headerSize int) error {
	gw.LogC(r.CvmfsReceiver.ctx, "mock receiver", gw.LogWarn).
		Str("leaserPath", leasePath).
		Msg("Requested submit agains a testing mock, shortcircuit success")
	return nil
}
