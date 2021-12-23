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

func createReceiver(t *testing.T) Receiver {
	st := stats.NewStatisticsMgr()
	receiver, err := NewReceiver(context.TODO(), getReceiverPath(), false, st, "-w \"\"")
	if err != nil {
		t.Fatalf("could not start receiver: %v", err)
	}
	return receiver
}

func TestMain(m *testing.M) {
	if os.Getenv("INTEGRATION_TESTS") == "ON" {
		os.Exit(m.Run())
	}
}

func TestReceiverCycle(t *testing.T) {
	receiver := createReceiver(t)
	if err := receiver.Echo(); err != nil {
		t.Fatalf("echo request failed: %v", err)

	}
	if err := receiver.Quit(); err != nil {
		t.Fatalf("quit request failed: %v", err)
	}
}

func TestReceiverOnCrashWeReturnError(t *testing.T) {
	receiver := createReceiver(t)
	if err := receiver.Echo(); err != nil {
		t.Fatalf("echo request failed: %v", err)
	}
	if err := receiver.TestCrash(); err == nil {
		t.Fatalf("crash request failed: %v", err)
	}
}

func TestReceiverAfterCrashWeCanStillCallCommandAndTheyWillReturnAnError(t *testing.T) {
	receiver := createReceiver(t)
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

// redundant test, but it mimic a problem we found in production.
// after a crash the .Quit() was hanging
func TestReceiverAfterCrashQuitDoesNotHang(t *testing.T) {
	receiver := createReceiver(t)

	receiver.TestCrash()

	if receiver.Quit() == nil {
		t.Fatalf("quit after crash didn't return nil")
	}
}
