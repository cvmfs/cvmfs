package backend

import (
	"io/ioutil"
	"os"
	"strings"
	"testing"
	"time"
)

func startBackend() *Services {
	tmp, err := ioutil.TempDir("", "test_lease_actions")
	if err != nil {
		os.Exit(1)
	}
	cfg := testConfig(tmp)

	ac := emptyAccessConfig()
	rd := strings.NewReader(accessConfigV2)
	if err := ac.load(rd, mockKeyImporter); err != nil {
		os.Exit(2)
	}

	ldb, err := NewLeaseDB("embedded", cfg)

	return &Services{Access: ac, Leases: ldb, Config: *cfg}
}

func TestLeaseActionsNewLease(t *testing.T) {
	backend := startBackend()
	t.Run("new lease busy", func(t *testing.T) {
		backend.Config.MaxLeaseTime = 1 * time.Second
		keyID := "keyid1"
		leasePath := "test2.repo.org/some/path"
		token1, err := NewLease(backend, keyID, leasePath)
		if err != nil {
			t.Fatalf("could not obtain new lease: %v", err)
		}
		defer CancelLease(backend, token1)
		token2, err := NewLease(backend, keyID, leasePath)
		if err == nil {
			CancelLease(backend, token2)
			t.Fatalf("new lease should not have been granted for busy path")
		}
	})
	t.Run("new lease expired", func(t *testing.T) {
		backend.Config.MaxLeaseTime = 1 * time.Microsecond
		keyID := "keyid1"
		leasePath := "test2.repo.org/some/path"
		token1, err := NewLease(backend, keyID, leasePath)
		if err != nil {
			t.Fatalf("could not obtain new lease: %v", err)
		}
		defer CancelLease(backend, token1)
		time.Sleep(backend.Config.MaxLeaseTime)
		if _, err := NewLease(backend, keyID, leasePath); err != nil {
			t.Fatalf("could not obtain new lease: %v", err)
		}
	})
	defer backend.Close()
}
