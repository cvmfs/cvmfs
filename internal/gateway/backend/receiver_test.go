package backend

import (
	"testing"
)

func TestReceiverCycle(t *testing.T) {
	receiver, err := NewReceiver("/usr/bin/cvmfs_receiver", true)
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
