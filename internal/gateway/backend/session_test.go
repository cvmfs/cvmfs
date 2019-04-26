package backend

import (
	"os"
	"testing"
	"time"
)

func TestMain(m *testing.M) {
	backend := StartTestBackend("session_test", 1*time.Second)
	defer backend.Stop()

	os.Exit(m.Run())
}

func TestCompleteSession(t *testing.T) {
}
