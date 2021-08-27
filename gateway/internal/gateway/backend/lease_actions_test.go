package backend

import (
	"context"
	"os"
	"path"
	"testing"
	"time"

	gw "github.com/cvmfs/gateway/internal/gateway"
)

func TestLeaseActionsNewLease(t *testing.T) {
	lastProtocolVersion := 3
	backend, tmp := StartTestBackend("lease_actions_test", 1*time.Second)
	defer func() {
		backend.Stop()
		os.RemoveAll(tmp)
	}()

	t.Run("new lease busy", func(t *testing.T) {
		backend.Config.MaxLeaseTime = 1 * time.Second
		keyID := "keyid1"
		leasePath := "test2.repo.org/some/path"
		token1, err := backend.NewLease(context.TODO(), keyID, leasePath, lastProtocolVersion)
		if err != nil {
			t.Fatalf("could not obtain new lease: %v", err)
		}
		defer backend.CancelLease(context.TODO(), token1)
		token2, err := backend.NewLease(context.TODO(), keyID, leasePath, lastProtocolVersion)
		if err == nil {
			backend.CancelLease(context.TODO(), token2)
			t.Fatalf("new lease should not have been granted for busy path")
		}
	})
	t.Run("new lease expired", func(t *testing.T) {
		backend.Config.MaxLeaseTime = 1 * time.Microsecond
		keyID := "keyid1"
		leasePath := "test2.repo.org/some/path"
		token1, err := backend.NewLease(context.TODO(), keyID, leasePath, lastProtocolVersion)
		if err != nil {
			t.Fatalf("could not obtain new lease: %v", err)
		}
		defer backend.CancelLease(context.TODO(), token1)
		time.Sleep(backend.Config.MaxLeaseTime)
		if _, err := backend.NewLease(context.TODO(), keyID, leasePath, lastProtocolVersion); err != nil {
			t.Fatalf("could not obtain new lease: %v", err)
		}
	})
	t.Run("new lease conflict", func(t *testing.T) {
		backend.Config.MaxLeaseTime = 1 * time.Second
		keyID := "keyid1"
		leasePath := "test2.repo.org/some/path"
		token1, err := backend.NewLease(context.TODO(), keyID, leasePath, lastProtocolVersion)
		if err != nil {
			t.Fatalf("could not obtain new lease: %v", err)
		}
		defer backend.CancelLease(context.TODO(), token1)
		token2, err := backend.NewLease(context.TODO(), keyID, leasePath+"/below", lastProtocolVersion)
		if err == nil {
			backend.CancelLease(context.TODO(), token2)
			t.Fatalf("new lease should not have been granted for conflicting path")
		}
	})
	t.Run("new lease invalid key", func(t *testing.T) {
		backend.Config.MaxLeaseTime = 1 * time.Second
		keyID := "keyidNO"
		leasePath := "test2.repo.org/some/path"
		token1, err := backend.NewLease(context.TODO(), keyID, leasePath, lastProtocolVersion)
		if err == nil {
			t.Fatalf("invalid key was accepted")
			backend.CancelLease(context.TODO(), token1)
		}
	})
	t.Run("new lease invalid repo", func(t *testing.T) {
		backend.Config.MaxLeaseTime = 1 * time.Second
		keyID := "keyid1"
		leasePath := "testNO.repo.org/some/path"
		token1, err := backend.NewLease(context.TODO(), keyID, leasePath, lastProtocolVersion)
		if err == nil {
			t.Fatalf("invalid repo for key was accepted")
			backend.CancelLease(context.TODO(), token1)
		}
	})
	t.Run("new lease invalid path", func(t *testing.T) {
		backend.Config.MaxLeaseTime = 1 * time.Second
		keyID := "keyid2"
		leasePath := "test2.repo.org/NO"
		token1, err := backend.NewLease(context.TODO(), keyID, leasePath, lastProtocolVersion)
		if err == nil {
			t.Fatalf("invalid path for key was accepted")
			backend.CancelLease(context.TODO(), token1)
		}
	})
}

func TestLeaseActionsCancelLease(t *testing.T) {
	lastProtocolVersion := 3
	backend, tmp := StartTestBackend("lease_actions_test", 1*time.Second)
	defer func() {
		backend.Stop()
		os.RemoveAll(tmp)
	}()

	t.Run("remove existing lease", func(t *testing.T) {
		backend.Config.MaxLeaseTime = 1 * time.Second
		keyID := "keyid1"
		leasePath := "test2.repo.org/some/path"
		token1, err := backend.NewLease(context.TODO(), keyID, leasePath, lastProtocolVersion)
		if err != nil {
			t.Fatalf("could not obtain new lease: %v", err)
		}
		if err := backend.CancelLease(context.TODO(), token1); err != nil {
			t.Fatalf("could not cancel existing lease: %v", err)
		}
	})
	t.Run("remove nonexisting lease", func(t *testing.T) {
		backend.Config.MaxLeaseTime = 1 * time.Second
		keyID := "keyid1"
		leasePath := "test2.repo.org/some/path"
		token1, err := backend.NewLease(context.TODO(), keyID, leasePath, lastProtocolVersion)
		if err != nil {
			t.Fatalf("could not obtain new lease: %v", err)
		}
		if err := backend.CancelLease(context.TODO(), token1); err != nil {
			t.Fatalf("could not cancel existing lease: %v", err)
		}
		if backend.CancelLease(context.TODO(), token1) == nil {
			t.Fatalf("cancel operation should have failed for nonexisting lease")
		}
	})
}

func TestLeaseActionsCancelLeaseByPath(t *testing.T) {
	lastProtocolVersion := 3
	backend, tmp := StartTestBackend("lease_actions_test", 1*time.Second)
	defer func() {
		backend.Stop()
		os.RemoveAll(tmp)
	}()

	backend.Config.MaxLeaseTime = 1 * time.Second
	keyID := "keyid1"
	prefix := "test2.repo.org/some"
	leasePath1 := path.Join(prefix, "path")
	leasePath2 := "test2.repo.org/another"
	if _, err := backend.NewLease(context.TODO(), keyID, leasePath1, lastProtocolVersion); err != nil {
		t.Fatalf("could not obtain new lease: %v", err)
	}
	if _, err := backend.NewLease(context.TODO(), keyID, leasePath2, lastProtocolVersion); err != nil {
		t.Fatalf("could not obtain new lease: %v", err)
	}
	if err := backend.CancelLeases(context.TODO(), prefix); err != nil {
		t.Fatalf("could not cancel existing lease: %v", err)
	}
	leases, _ := backend.GetLeases(context.TODO())
	if len(leases) > 1 {
		t.Fatalf("only one of the two existing leases should have been cancelled")
	}
}

func TestLeaseActionsGetLease(t *testing.T) {
	lastProtocolVersion := 3
	backend, tmp := StartTestBackend("lease_actions_test", 1*time.Second)
	defer func() {
		backend.Stop()
		os.RemoveAll(tmp)
	}()

	t.Run("get valid lease", func(t *testing.T) {
		backend.Config.MaxLeaseTime = 1 * time.Second
		keyID := "keyid1"
		leasePath := "test2.repo.org/some/path"
		token1, err := backend.NewLease(context.TODO(), keyID, leasePath, lastProtocolVersion)
		if err != nil {
			t.Fatalf("could not obtain new lease: %v", err)
		}
		lease, err := backend.GetLease(context.TODO(), token1)
		if err != nil {
			t.Fatalf("could not query existing lease: %v", err)
		}
		if lease.KeyID != keyID && lease.LeasePath != leasePath {
			t.Fatalf("lease query result is invalid: %v", lease)
		}
		defer backend.CancelLease(context.TODO(), token1)
	})
	t.Run("get expired lease", func(t *testing.T) {
		backend.Config.MaxLeaseTime = 1 * time.Microsecond
		keyID := "keyid1"
		leasePath := "test2.repo.org/some/path"
		token1, err := backend.NewLease(context.TODO(), keyID, leasePath, lastProtocolVersion)
		if err != nil {
			t.Fatalf("could not obtain new lease: %v", err)
		}
		time.Sleep(2 * backend.Config.MaxLeaseTime)
		_, err = backend.GetLease(context.TODO(), token1)
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
		_, err := backend.NewLease(context.TODO(), keyID, leasePath, lastProtocolVersion)
		if err != nil {
			t.Fatalf("could not obtain new lease: %v", err)
		}
		token2, err := NewLeaseToken(leasePath, time.Second)
		if err != nil {
			t.Fatalf("could not generate second token")
		}
		_, err = backend.GetLease(context.TODO(), token2.TokenStr)
		if err == nil {
			t.Fatalf("query should not succeed with invalid token: %v", err)
		}
		if _, ok := err.(InvalidLeaseError); !ok {
			t.Fatalf("query should have returned an InvalidLeaseError. Instead: %v", err)
		}
	})
}

func TestLeaseActionsCommitLease(t *testing.T) {
	lastProtocolVersion := 3
	backend, tmp := StartTestBackend("lease_actions_test", 1*time.Second)
	defer func() {
		backend.Stop()
		os.RemoveAll(tmp)
	}()

	t.Run("commit valid lease", func(t *testing.T) {
		backend.Config.MaxLeaseTime = 1 * time.Second
		keyID := "keyid1"
		leasePath := "test2.repo.org/some/path"
		token, err := backend.NewLease(context.TODO(), keyID, leasePath, lastProtocolVersion)
		if err != nil {
			t.Fatalf("could not obtain new lease: %v", err)
		}
		if _, err := backend.CommitLease(
			context.TODO(), token, "old_hash", "new_hash",
			gw.RepositoryTag{
				Name:        "mytag",
				Channel:     "mychannel",
				Description: "this is a tag",
			}); err != nil {
			t.Fatalf("could not commit existing lease: %v", err)
			backend.CancelLease(context.TODO(), token)
		}
	})
	t.Run("commit invalid lease", func(t *testing.T) {
		backend.Config.MaxLeaseTime = 1 * time.Second
		leasePath := "test2.repo.org/some/path"
		token, err := NewLeaseToken(leasePath, backend.Config.MaxLeaseTime)
		if err != nil {
			t.Fatalf("could not obtain new lease token: %v", err)
		}
		if _, err := backend.CommitLease(
			context.TODO(), token.TokenStr, "old_hash", "new_hash",
			gw.RepositoryTag{
				Name:        "mytag",
				Channel:     "mychannel",
				Description: "this is a tag",
			}); err == nil {
			t.Fatalf("invalid lease should not have been accepted for commit")
		}
	})
	t.Run("commit expired lease", func(t *testing.T) {
		backend.Config.MaxLeaseTime = 1 * time.Millisecond
		keyID := "keyid1"
		leasePath := "test2.repo.org/some/path"
		token, err := backend.NewLease(context.TODO(), keyID, leasePath, lastProtocolVersion)
		if err != nil {
			t.Fatalf("could not obtain new lease: %v", err)
		}
		time.Sleep(2 * backend.Config.MaxLeaseTime)
		if _, err := backend.CommitLease(
			context.TODO(), token, "old_hash", "new_hash",
			gw.RepositoryTag{
				Name:        "mytag",
				Channel:     "mychannel",
				Description: "this is a tag",
			}); err == nil {
			t.Fatalf("expired lease should not have been accepted for commit")
		}
	})
}
