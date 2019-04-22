package backend

import (
	"io/ioutil"
	"os"
	"strings"
	"testing"
	"time"
)

func startLeaseActionTestBackend() *Services {
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

	ldb, err := OpenLeaseDB("embedded", cfg)

	return &Services{Access: ac, Leases: ldb, Config: *cfg}
}

func TestLeaseActionsNewLease(t *testing.T) {
	backend := startLeaseActionTestBackend()

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
	t.Run("new lease conflict", func(t *testing.T) {
		backend.Config.MaxLeaseTime = 1 * time.Second
		keyID := "keyid1"
		leasePath := "test2.repo.org/some/path"
		token1, err := NewLease(backend, keyID, leasePath)
		if err != nil {
			t.Fatalf("could not obtain new lease: %v", err)
		}
		defer CancelLease(backend, token1)
		token2, err := NewLease(backend, keyID, leasePath+"/below")
		if err == nil {
			CancelLease(backend, token2)
			t.Fatalf("new lease should not have been granted for conflicting path")
		}
	})
	defer backend.Stop()
}
func TestLeaseActionsCancelLease(t *testing.T) {
	backend := startLeaseActionTestBackend()

	t.Run("remove existing lease", func(t *testing.T) {
		backend.Config.MaxLeaseTime = 1 * time.Second
		keyID := "keyid1"
		leasePath := "test2.repo.org/some/path"
		token1, err := NewLease(backend, keyID, leasePath)
		if err != nil {
			t.Fatalf("could not obtain new lease: %v", err)
		}
		if err := CancelLease(backend, token1); err != nil {
			t.Fatalf("could not cancel existing lease: %v", err)
		}
	})
	t.Run("remove nonexisting lease", func(t *testing.T) {
		backend.Config.MaxLeaseTime = 1 * time.Second
		keyID := "keyid1"
		leasePath := "test2.repo.org/some/path"
		token1, err := NewLease(backend, keyID, leasePath)
		if err != nil {
			t.Fatalf("could not obtain new lease: %v", err)
		}
		if err := CancelLease(backend, token1); err != nil {
			t.Fatalf("could not cancel existing lease: %v", err)
		}
		if CancelLease(backend, token1) == nil {
			t.Fatalf("cancel operation should have failed for nonexisting lease")
		}
	})
	defer backend.Stop()
}

func TestLeaseActionsGetLease(t *testing.T) {
	backend := startLeaseActionTestBackend()

	t.Run("get valid lease", func(t *testing.T) {
		backend.Config.MaxLeaseTime = 1 * time.Second
		keyID := "keyid1"
		leasePath := "test2.repo.org/some/path"
		token1, err := NewLease(backend, keyID, leasePath)
		if err != nil {
			t.Fatalf("could not obtain new lease: %v", err)
		}
		lease, err := GetLease(backend, token1)
		if err != nil {
			t.Fatalf("could not query existing lease: %v", err)
		}
		if lease.KeyID != keyID && lease.LeasePath != leasePath {
			t.Fatalf("lease query result is invalid: %v", lease)
		}
		defer CancelLease(backend, token1)
	})
	t.Run("get expired lease", func(t *testing.T) {
		backend.Config.MaxLeaseTime = 1 * time.Microsecond
		keyID := "keyid1"
		leasePath := "test2.repo.org/some/path"
		token1, err := NewLease(backend, keyID, leasePath)
		if err != nil {
			t.Fatalf("could not obtain new lease: %v", err)
		}
		time.Sleep(2 * backend.Config.MaxLeaseTime)
		_, err = GetLease(backend, token1)
		if err == nil {
			t.Fatalf("query should not succeed for expired leases: %v", err)
		}
		if _, ok := err.(ExpiredTokenError); !ok {
			t.Fatalf("query should have returned an ExpiredTokenError. Instead: %v", err)
		}
	})
	t.Run("get invalid lease", func(t *testing.T) {
		backend.Config.MaxLeaseTime = 1 * time.Second
		keyID := "keyid1"
		leasePath := "test2.repo.org/some/path"
		_, err := NewLease(backend, keyID, leasePath)
		if err != nil {
			t.Fatalf("could not obtain new lease: %v", err)
		}
		token2, err := NewLeaseToken(leasePath, time.Second)
		if err != nil {
			t.Fatalf("could not generate second token")
		}
		_, err = GetLease(backend, token2.TokenStr)
		if err == nil {
			t.Fatalf("query should not succeed with invalid token: %v", err)
		}
		if _, ok := err.(InvalidLeaseError); !ok {
			t.Fatalf("query should have returned an InvalidLeaseError. Instead: %v", err)
		}
	})
	defer backend.Stop()
}
