package receiver

import (
	"context"
	"io"

	gw "github.com/cvmfs/gateway/internal/gateway"
	stats "github.com/cvmfs/gateway/internal/gateway/statistics"
)

// MockReceiver is a mocked implementation of the Receiver interface, for testing
// Can implement fault injection
type MockReceiver struct {
	CvmfsReceiver
}

// NewMockReceiver constructs a new MockReceiver object which implements the
// Receiver interface
func NewMockReceiver(ctx context.Context, execPath string, args ...string) (Receiver, error) {
	CvmfsReceiver, err := NewCvmfsReceiver(ctx, execPath, stats.NewStatisticsMgr(), args...)
	// if some error happens, like the receiver code cannot be started,
	// we want to handle it separately, otherwise it crash with Segmentation Fault.
	if err != nil {
		return &MockReceiver{}, err
	}
	return &MockReceiver{*CvmfsReceiver}, nil
}

func (r *MockReceiver) Commit(leasePath, oldRootHash, newRootHash string, tag gw.RepositoryTag) (uint64, error) {
	gw.LogC(r.CvmfsReceiver.ctx, "mock receiver", gw.LogWarn).
		Str("leaserPath", leasePath).
		Msg("Requested commit agains a testing mock, shortcircuit success")
	return 1, nil
}

func (r *MockReceiver) SubmitPayload(leasePath string, payload io.Reader, digest string, headerSize int) error {
	gw.LogC(r.CvmfsReceiver.ctx, "mock receiver", gw.LogWarn).
		Str("leaserPath", leasePath).
		Msg("Requested submit agains a testing mock, shortcircuit success")
	return nil
}

func (r *MockReceiver) Interrupt() error {
	return nil
}
