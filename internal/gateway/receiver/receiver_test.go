package receiver

import (
	"context"
	"testing"

	stats "github.com/cvmfs/gateway/internal/gateway/statistics"
)

func TestReceiverCycle(t *testing.T) {
	st := stats.NewStatisticsMgr()
	receiver, err := NewReceiver(context.TODO(), "/usr/bin/cvmfs_receiver", true, st)
	if err != nil {
		t.Fatalf("could not start receiver: %v", err)
	}
	if err := receiver.Echo(); err != nil {
		t.Fatalf("echo request failed: %v", err)
	}
	if err := receiver.Quit(); err != nil {
		t.Fatalf("quit request failed: %v", err)
	}
}
