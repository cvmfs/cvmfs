//go:build integration

package receiver

import (
	"context"
	"os"
	"testing"

	stats "github.com/cvmfs/gateway/internal/gateway/statistics"
)

func getReceiverPath() string {
	receiverPath := os.Getenv("CVMFS_RECEIVER_PATH")
	if receiverPath == "" {
		receiverPath = "/usr/bin/cvmfs_receiver"
	}
	return receiverPath
}

func TestReceiverCycle(t *testing.T) {
	st := stats.NewStatisticsMgr()
	receiver, err := NewReceiver(context.TODO(), getReceiverPath(), false, st)
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

func TestReceiverOnCrashWeReturnError(t *testing.T) {
	receiver, err := NewReceiver(context.TODO(), getReceiverPath(), false, stats.NewStatisticsMgr())
	if err != nil {
		t.Fatalf("could not start receiver: %v", err)
	}
	if err := receiver.Echo(); err != nil {
		t.Fatalf("echo request failed: %v", err)
	}
	err = receiver.TestCrash()
	// note how we check the err being equal (==) and not different (!=) to nil
	if err == nil {
		t.Fatalf("crash request failed: %v", err)
	}
}

func TestReceiverAfterCrashWeCanStillCallCommandAndTheyWillReturnAnError(t *testing.T) {
	receiver, err := NewReceiver(context.TODO(), getReceiverPath(), false, stats.NewStatisticsMgr())
	if err != nil {
		t.Fatalf("could not start receiver: %v", err)
	}
	if err := receiver.Echo(); err != nil {
		t.Fatalf("echo request failed: %v", err)
	}

	receiver.TestCrash()

	if receiver.Echo() == nil {
		t.Fatalf("echo after crash didn't return nil")
	}

	if receiver.Quit() == nil {
		t.Fatalf("quit after crash didn't return nil")
	}
}

// reduntat test, but it mimic a problem we found in production.
// after a crash the .Quit() was hanging
func TestReceiverAfterCrashQuitDoesNotHang(t *testing.T) {
	receiver, err := NewReceiver(context.TODO(), getReceiverPath(), false, stats.NewStatisticsMgr())
	if err != nil {
		t.Fatalf("could not start receiver: %v", err)
	}

	receiver.TestCrash()

	if receiver.Quit() == nil {
		t.Fatalf("quit after crash didn't return nil")
	}
}
