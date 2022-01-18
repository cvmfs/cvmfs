package backend

import (
	"bytes"
	"context"
	"errors"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/cvmfs/gateway/internal/gateway"
)

func TestLeaseDBOpen(t *testing.T) {
	tmp, err := ioutil.TempDir("", "test_lease_db")
	if err != nil {
		t.Fatalf("could not create temp dir for test case")
	}
	defer os.RemoveAll(tmp)

	cfg := gateway.Config{WorkDir: tmp}
	db, err := OpenDB(cfg)
	if err != nil {
		t.Fatalf("could not create database: %v", err)
	}
	defer db.Close()
}

func TestLeaseDBCRUD(t *testing.T) {
	lastProtocolVersion := 3
	tmp, err := ioutil.TempDir("", "test_lease_db")
	if err != nil {
		t.Fatalf("could not create temp dir for test case")
	}
	defer os.RemoveAll(tmp)

	cfg := gateway.Config{WorkDir: tmp}
	db, err := OpenDB(cfg)
	if err != nil {
		t.Fatalf("could not create database: %v", err)
	}
	defer db.Close()

	keyID1 := "key1"
	leasePath1 := "test.repo.org/path/one"
	token1 := NewLeaseToken()
	t.Run("new lease", func(t *testing.T) {
		if err != nil {
			t.Fatalf("could not generate session token: %v", err)
		}

		if err := NewLease(context.TODO(), db, keyID1, leasePath1, lastProtocolVersion, *token1); err != nil {
			t.Fatalf("could not add new lease: %v", err)
		}
	})
	t.Run("get leases", func(t *testing.T) {
		leases, err := GetLeases(context.TODO(), db)
		if err != nil {
			t.Fatalf("could not retrieve leases: %v", err)
		}
		if len(leases) != 1 {
			t.Fatalf("expected 1 lease")
		}
		_, present := leases[leasePath1]
		if !present {
			t.Fatalf("missing lease for %v", leasePath1)
		}
	})
	t.Run("get lease for token", func(t *testing.T) {
		_, lease, err := GetLease(context.TODO(), db, token1.TokenStr)
		if err != nil {
			t.Fatalf("could not retrieve leases: %v", err)
		}
		if lease.KeyID != keyID1 ||
			lease.Token != token1 ||
			!bytes.Equal(lease.Token.Secret, token1.Secret) {
			t.Fatalf("invalid lease returned: %v", lease)
		}
	})
	t.Run("cancel leases", func(t *testing.T) {
		leasePath1 := "test.repo.org/path/two"
		token1, err := NewLeaseToken(leasePath1, TestMaxLeaseTime)
		if err != nil {
			t.Fatalf("could not generate session token: %v", err)
		}
		if err := NewLease(context.TODO(), db, keyID1, leasePath1, lastProtocolVersion, *token1); err != nil {
			t.Fatalf("could not add new lease: %v", err)
		}
		leasePath2 := "test.repo.org/another/path"
		token2, err := NewLeaseToken(leasePath2, TestMaxLeaseTime)
		if err != nil {
			t.Fatalf("could not generate session token: %v", err)
		}
		if err := NewLease(context.TODO(), db, keyID1, leasePath2, lastProtocolVersion, *token2); err != nil {
			t.Fatalf("could not add new lease: %v", err)
		}

		if err := CancelLeases(context.TODO(), db, "test.repo.org/path"); err != nil {
			t.Fatalf("could not cancel all leases: %v", err)
		}

		leases, err := GetLeases(context.TODO(), db)
		if err != nil {
			t.Fatalf("could not retrieve leases: %v", err)
		}
		if len(leases) > 1 {
			t.Fatalf("remaining leases after cancellation")
		}

		if err := CancelLeases(context.TODO(), db, "test.repo.org/"); err != nil {
			t.Fatalf("could not cancel all leases: %v", err)
		}
	})
	t.Run("clear lease for token", func(t *testing.T) {
		leasePath := "test.repo.org/path/three"
		token, err := NewLeaseToken(leasePath, TestMaxLeaseTime)
		if err != nil {
			t.Fatalf("could not generate session token: %v", err)
		}

		if err := NewLease(context.TODO(), db, keyID1, leasePath, lastProtocolVersion, *token); err != nil {
			t.Fatalf("could not add new lease: %v", err)
		}

		if err := CancelLease(context.TODO(), db, token.TokenStr); err != nil {
			t.Fatalf("could not clear lease for token")
		}

		leases, err := GetLeases(context.TODO(), db)
		if err != nil {
			t.Fatalf("could not retrieve leases: %v", err)
		}
		if len(leases) > 0 {
			t.Fatalf("remaining leases after cancellation")
		}
	})
}

func TestLeaseDBConflicts(t *testing.T) {
	lastProtocolVersion := 3
	tmp, err := ioutil.TempDir("", "test_lease_db")
	if err != nil {
		t.Fatalf("could not create temp dir for test case")
	}
	defer os.RemoveAll(tmp)

	cfg := gateway.Config{WorkDir: tmp}
	db, err := OpenDB(cfg)
	if err != nil {
		t.Fatalf("could not create database: %v", err)
	}
	defer db.Close()

	keyID := "key1"
	leasePath1 := "test.repo.org/path/one"
	token1, err := NewLeaseToken(leasePath1, TestMaxLeaseTime)
	if err != nil {
		t.Fatalf("could not generate session token: %v", err)
	}

	if err := NewLease(context.TODO(), db, keyID, leasePath1, lastProtocolVersion, *token1); err != nil {
		t.Fatalf("could not add new lease: %v", err)
	}

	leasePath2 := "test.repo.org/path"
	token2, err := NewLeaseToken(leasePath2, TestMaxLeaseTime)
	if err != nil {
		t.Fatalf("could not generate session token: %v", err)
	}

	err = NewLease(context.TODO(), db, keyID, leasePath2, lastProtocolVersion, *token2)
	if !errors.As(err, &PathBusyError{}) {
		t.Fatalf("conflicting lease was added for path: %v", leasePath2)
	}

	leasePath3 := "test.repo.org/path/one/below"
	token3, err := NewLeaseToken(leasePath3, TestMaxLeaseTime)
	if err != nil {
		t.Fatalf("could not generate session token: %v", err)
	}

	err = NewLease(context.TODO(), db, keyID, leasePath3, lastProtocolVersion, *token3)
	if !errors.As(err, &PathBusyError{}) {
		t.Fatalf("conflicting lease was added for path: %v", leasePath3)
	}
}

func TestLeaseDBExpired(t *testing.T) {
	lastProtocolVersion := 3
	tmp, err := ioutil.TempDir("", "test_lease_db")
	if err != nil {
		t.Fatalf("could not create temp dir for test case")
	}
	defer os.RemoveAll(tmp)

	shortLeaseTime := 1 * time.Millisecond

	cfg := gateway.Config{WorkDir: tmp}
	db, err := OpenDB(cfg)
	if err != nil {
		t.Fatalf("could not create database: %v", err)
	}
	defer db.Close()

	keyID := "key1"
	leasePath := "test.repo.org/path/one"
	token1, err := NewLeaseToken(leasePath, shortLeaseTime)
	if err != nil {
		t.Fatalf("could not generate session token: %v", err)
	}

	if err := NewLease(context.TODO(), db, keyID, leasePath, lastProtocolVersion, *token1); err != nil {
		t.Fatalf("could not add new lease: %v", err)
	}

	time.Sleep(2 * shortLeaseTime)

	token2, err := NewLeaseToken(leasePath, shortLeaseTime)
	if err != nil {
		t.Fatalf("could not generate session token: %v", err)
	}

	if err := NewLease(context.TODO(), db, keyID, leasePath, lastProtocolVersion, *token2); err != nil {
		t.Fatalf("could not add new lease in place of expired one")
	}
}
