package backend

import (
	"context"
	"errors"
	"os"
	"path"
	"testing"
	"time"

	gw "github.com/cvmfs/gateway/internal/gateway"
	"github.com/cvmfs/gateway/internal/gateway/receiver"
)

func TestLeaseServiceNewLease(t *testing.T) {
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
		token1, err := backend.NewLease(context.TODO(), keyID, leasePath, "host", lastProtocolVersion)
		if err != nil {
			t.Fatalf("could not obtain new lease: %v", err)
		}
		defer backend.CancelLease(context.TODO(), token1)
		token2, err := backend.NewLease(context.TODO(), keyID, leasePath, "host", lastProtocolVersion)
		if err == nil {
			backend.CancelLease(context.TODO(), token2)
			t.Fatalf("new lease should not have been granted for busy path")
		}
	})
	t.Run("new lease expired", func(t *testing.T) {
		backend.Config.MaxLeaseTime = 1 * time.Microsecond
		keyID := "keyid1"
		leasePath := "test2.repo.org/some/path"
		token1, err := backend.NewLease(context.TODO(), keyID, leasePath, "host", lastProtocolVersion)
		if err != nil {
			t.Fatalf("could not obtain new lease: %v", err)
		}
		defer backend.CancelLease(context.TODO(), token1)
		time.Sleep(backend.Config.MaxLeaseTime)
		if _, err := backend.NewLease(context.TODO(), keyID, leasePath, "host", lastProtocolVersion); err != nil {
			t.Fatalf("could not obtain new lease: %v", err)
		}
	})
	t.Run("new lease conflict", func(t *testing.T) {
		backend.Config.MaxLeaseTime = 1 * time.Second
		keyID := "keyid1"
		leasePath := "test2.repo.org/some/path"
		token1, err := backend.NewLease(context.TODO(), keyID, leasePath, "host", lastProtocolVersion)
		if err != nil {
			t.Fatalf("could not obtain new lease: %v", err)
		}
		defer backend.CancelLease(context.TODO(), token1)
		token2, err := backend.NewLease(context.TODO(), keyID, leasePath+"/below", "host", lastProtocolVersion)
		if err == nil {
			backend.CancelLease(context.TODO(), token2)
			t.Fatalf("new lease should not have been granted for conflicting path")
		}
	})
	t.Run("new lease invalid key", func(t *testing.T) {
		backend.Config.MaxLeaseTime = 1 * time.Second
		keyID := "keyidNO"
		leasePath := "test2.repo.org/some/path"
		token1, err := backend.NewLease(context.TODO(), keyID, leasePath, "host", lastProtocolVersion)
		if err == nil {
			backend.CancelLease(context.TODO(), token1)
			t.Fatalf("invalid key was accepted")
		}
	})
	t.Run("new lease invalid repo", func(t *testing.T) {
		backend.Config.MaxLeaseTime = 1 * time.Second
		keyID := "keyid1"
		leasePath := "testNO.repo.org/some/path"
		token1, err := backend.NewLease(context.TODO(), keyID, leasePath, "host", lastProtocolVersion)
		if err == nil {
			backend.CancelLease(context.TODO(), token1)
			t.Fatalf("invalid repo for key was accepted")
		}
	})
	t.Run("new lease invalid path", func(t *testing.T) {
		backend.Config.MaxLeaseTime = 1 * time.Second
		keyID := "keyid2"
		leasePath := "test2.repo.org/NO"
		token1, err := backend.NewLease(context.TODO(), keyID, leasePath, "host", lastProtocolVersion)
		if err == nil {
			backend.CancelLease(context.TODO(), token1)
			t.Fatalf("invalid path for key was accepted")
		}
	})
}

func TestLeaseServiceCancelLease(t *testing.T) {
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
		token1, err := backend.NewLease(context.TODO(), keyID, leasePath, "host", lastProtocolVersion)
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
		token1, err := backend.NewLease(context.TODO(), keyID, leasePath, "host", lastProtocolVersion)
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

func TestLeaseServiceCancelLeaseByPath(t *testing.T) {
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
	if _, err := backend.NewLease(context.TODO(), keyID, leasePath1, "host", lastProtocolVersion); err != nil {
		t.Fatalf("could not obtain new lease: %v", err)
	}
	if _, err := backend.NewLease(context.TODO(), keyID, leasePath2, "host", lastProtocolVersion); err != nil {
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

func TestLeaseServiceGetLease(t *testing.T) {
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
		token1, err := backend.NewLease(context.TODO(), keyID, leasePath, "host", lastProtocolVersion)
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
		token1, err := backend.NewLease(context.TODO(), keyID, leasePath, "host", lastProtocolVersion)
		if err != nil {
			t.Fatalf("could not obtain new lease: %v", err)
		}
		time.Sleep(2 * backend.Config.MaxLeaseTime)
		_, err = backend.GetLease(context.TODO(), token1)
		if err == nil {
			t.Fatalf("query should not succeed for expired leases: %v", err)
		}
		if !errors.As(err, &InvalidLeaseError{}) {
			t.Fatalf("query should have returned an InvalidLeaseError. Instead: %v", err)
		}
	})
	t.Run("get invalid lease", func(t *testing.T) {
		backend.Config.MaxLeaseTime = 1 * time.Second
		keyID := "keyid1"
		leasePath := "test2.repo.org/some/path"
		_, err := backend.NewLease(context.TODO(), keyID, leasePath, "host", lastProtocolVersion)
		if err != nil {
			t.Fatalf("could not obtain new lease: %v", err)
		}
		token2 := NewLeaseToken()
		if err != nil {
			t.Fatalf("could not generate second token")
		}
		_, err = backend.GetLease(context.TODO(), token2)
		if err == nil {
			t.Fatalf("query should not succeed with invalid token: %v", err)
		}
		if !errors.As(err, &InvalidLeaseError{}) {
			t.Fatalf("query should have returned an InvalidLeaseError. Instead: %v", err)
		}
	})
}

func TestLeaseServiceCommitLease(t *testing.T) {
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
		token, err := backend.NewLease(context.TODO(), keyID, leasePath, "host", lastProtocolVersion)
		if err != nil {
			t.Fatalf("could not obtain new lease: %v", err)
		}
		if _, err := backend.CommitLease(
			context.TODO(), token, "old_hash", "new_hash",
			gw.RepositoryTag{
				Name:        "mytag",
				Description: "this is a tag",
			}); err != nil {
			t.Fatalf("could not commit existing lease: %v", err)
			backend.CancelLease(context.TODO(), token)
		}
	})
	t.Run("commit invalid lease", func(t *testing.T) {
		backend.Config.MaxLeaseTime = 1 * time.Second
		token := NewLeaseToken()
		if _, err := backend.CommitLease(
			context.TODO(), token, "old_hash", "new_hash",
			gw.RepositoryTag{
				Name:        "mytag",
				Description: "this is a tag",
			}); err == nil {
			t.Fatalf("invalid lease should not have been accepted for commit")
		}
	})
	t.Run("commit expired lease", func(t *testing.T) {
		backend.Config.MaxLeaseTime = 1 * time.Millisecond
		keyID := "keyid1"
		leasePath := "test2.repo.org/some/path"
		token, err := backend.NewLease(context.TODO(), keyID, leasePath, "host", lastProtocolVersion)
		if err != nil {
			t.Fatalf("could not obtain new lease: %v", err)
		}
		time.Sleep(2 * backend.Config.MaxLeaseTime)
		if _, err := backend.CommitLease(
			context.TODO(), token, "old_hash", "new_hash",
			gw.RepositoryTag{
				Name:        "mytag",
				Description: "this is a tag",
			}); err == nil {
			t.Fatalf("expired lease should not have been accepted for commit")
		}
	})
}

// TestCommitLeaseDoesNotBlockGetLeases is a regression test for issue #4103: a
// slow commit must not hold leaseMutex while the external receiver is running,
// otherwise lease listing blocks for the whole commit duration.
func TestCommitLeaseDoesNotBlockGetLeases(t *testing.T) {
	backend, tmp := StartTestBackend("lease_concurrency_test", 10*time.Second)
	defer func() {
		backend.Stop()
		os.RemoveAll(tmp)
	}()

	keyID := "keyid1"
	leasePath := "test2.repo.org/some/path"
	token, err := backend.NewLease(context.TODO(), keyID, leasePath, "host", 3)
	if err != nil {
		t.Fatalf("could not obtain new lease: %v", err)
	}

	// Gate the mock receiver so the commit parks in flight, holding only the
	// per-repository lock (not leaseMutex).
	gate := make(chan struct{})
	entered := receiver.SetMockCommitGate(gate)
	defer receiver.SetMockCommitGate(nil)

	commitDone := make(chan struct{})
	go func() {
		backend.CommitLease(context.TODO(), token, "old_hash", "new_hash", gw.RepositoryTag{Name: "mytag"})
		close(commitDone)
	}()

	// Wait until the commit is actually parked inside the receiver, so that on
	// the buggy code leaseMutex is provably held while we time GetLeases.
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("commit never reached the receiver")
	}

	// GetLeases must return promptly even while the commit is parked, since it
	// no longer waits on leaseMutex held across the external commit.
	getLeasesDone := make(chan struct{})
	go func() {
		if _, err := backend.GetLeases(context.TODO()); err != nil {
			t.Errorf("GetLeases failed: %v", err)
		}
		close(getLeasesDone)
	}()

	select {
	case <-getLeasesDone:
	case <-time.After(2 * time.Second):
		t.Fatal("GetLeases blocked behind an in-flight CommitLease (issue #4103 regression)")
	}

	// Release the parked commit and let it finish cleanly.
	close(gate)
	<-commitDone
}
