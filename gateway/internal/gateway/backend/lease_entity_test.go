package backend

import (
	"context"
	"database/sql"
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/cvmfs/gateway/internal/gateway"
)

func TestLeaseOpen(t *testing.T) {
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

func withTx(ctx context.Context, db *sql.DB, t *testing.T, test func(ctx context.Context, tx *sql.Tx) error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("could not begin transaction: %v", err)
	}
	defer tx.Rollback()

	if err := test(ctx, tx); err != nil {
		t.Fatalf("test failed: %v", err)
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("could not commit transaction: %v", err)
	}
}

func TestLeaseCRUD(t *testing.T) {
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

	ctx := context.Background()

	keyID1 := "key1"
	leasePath1 := "test.repo.org/path/one"
	token1 := NewLeaseToken()
	t.Run("new lease", func(t *testing.T) {
		withTx(ctx, db.SQL, t, func(ctx context.Context, tx *sql.Tx) error {
			lease := Lease{
				Token:           token1,
				Repository:      "test.repo.org",
				Path:            "path/one",
				KeyID:           keyID1,
				Expiration:      time.Now().Add(TestMaxLeaseTime),
				ProtocolVersion: lastProtocolVersion,
			}

			if err := CreateLease(ctx, tx, lease); err != nil {
				return err
			}

			return nil
		})
	})
	t.Run("get leases", func(t *testing.T) {
		withTx(ctx, db.SQL, t, func(ctx context.Context, tx *sql.Tx) error {
			leases, err := FindAllActiveLeases(ctx, tx)
			if err != nil {
				return fmt.Errorf("could not retrieve leases: %w", err)
			}
			if len(leases) != 1 {
				return fmt.Errorf("expected 1 lease, found: %v", len(leases))
			}
			if leases[0].CombinedLeasePath() != leasePath1 {
				return fmt.Errorf("missing lease for %v", leasePath1)
			}

			return nil
		})
	})
	t.Run("get lease for token", func(t *testing.T) {
		withTx(ctx, db.SQL, t, func(ctx context.Context, tx *sql.Tx) error {
			_, err := FindLeaseByToken(ctx, tx, token1)
			if err != nil {
				return fmt.Errorf("could not retrieve leases: %w", err)
			}

			return nil
		})
	})
	t.Run("cancel leases", func(t *testing.T) {
		repo := "test.repo.org"
		withTx(ctx, db.SQL, t, func(ctx context.Context, tx *sql.Tx) error {
			path1 := "path/two"
			token1 := NewLeaseToken()
			lease1 := Lease{
				Token:           token1,
				Repository:      repo,
				Path:            path1,
				KeyID:           keyID1,
				Expiration:      time.Now().Add(TestMaxLeaseTime),
				ProtocolVersion: lastProtocolVersion,
			}
			if err := CreateLease(ctx, tx, lease1); err != nil {
				return fmt.Errorf("could not add new lease: %w", err)
			}
			path2 := "another/path"
			token2 := NewLeaseToken()
			lease2 := Lease{
				Token:           token2,
				Repository:      repo,
				Path:            path2,
				KeyID:           keyID1,
				Expiration:      time.Now().Add(TestMaxLeaseTime),
				ProtocolVersion: lastProtocolVersion,
			}
			if err := CreateLease(ctx, tx, lease2); err != nil {
				return fmt.Errorf("could not add new lease: %w", err)
			}

			if err := DeleteAllLeasesByRepositoryAndPathPrefix(ctx, tx, repo, "path"); err != nil {
				return fmt.Errorf("could not cancel all leases: %w", err)
			}

			leases, err := FindAllLeases(ctx, tx)
			if err != nil {
				return fmt.Errorf("could not retrieve leases: %w", err)
			}
			if len(leases) > 1 {
				return fmt.Errorf("remaining leases after cancellation")
			}

			if err := DeleteAllLeasesByRepository(ctx, tx, repo); err != nil {
				return fmt.Errorf("could not cancel all leases: %w", err)
			}

			return nil
		})
	})
	t.Run("clear lease for token", func(t *testing.T) {
		withTx(ctx, db.SQL, t, func(ctx context.Context, tx *sql.Tx) error {
			repo := "test.repo.org"
			path := "path/three"
			token := NewLeaseToken()

			lease := Lease{
				Token:           token,
				Repository:      repo,
				Path:            path,
				KeyID:           keyID1,
				Expiration:      time.Now().Add(TestMaxLeaseTime),
				ProtocolVersion: lastProtocolVersion,
			}

			if err := CreateLease(ctx, tx, lease); err != nil {
				return fmt.Errorf("could not add new lease: %w", err)
			}

			if err := DeleteLeaseByToken(ctx, tx, token); err != nil {
				return fmt.Errorf("could not clear lease for token")
			}

			leases, err := FindAllLeases(ctx, tx)
			if err != nil {
				return fmt.Errorf("could not retrieve leases: %w", err)
			}
			if len(leases) > 0 {
				return fmt.Errorf("remaining leases after cancellation")
			}

			return nil
		})
	})
}

func TestLeaseConflicts(t *testing.T) {
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
	repo := "test.repo.org"
	path1 := "path/one"

	withTx(context.Background(), db.SQL, t, func(ctx context.Context, tx *sql.Tx) error {
		token1 := NewLeaseToken()

		lease1 := Lease{
			Token:           token1,
			Repository:      repo,
			Path:            path1,
			KeyID:           keyID,
			Expiration:      time.Now().Add(TestMaxLeaseTime),
			ProtocolVersion: lastProtocolVersion,
		}

		if err := CreateLease(ctx, tx, lease1); err != nil {
			return fmt.Errorf("could not add new lease: %w", err)
		}

		path2 := "path"
		token2 := NewLeaseToken()

		lease2 := Lease{
			Token:           token2,
			Repository:      repo,
			Path:            path2,
			KeyID:           keyID,
			Expiration:      time.Now().Add(TestMaxLeaseTime),
			ProtocolVersion: lastProtocolVersion,
		}

		if err := CreateLease(ctx, tx, lease2); err != nil {
			return fmt.Errorf("could not add new lease: %w", err)
		}

		path3 := "path/one/below"
		token3 := NewLeaseToken()

		lease3 := Lease{
			Token:           token3,
			Repository:      repo,
			Path:            path3,
			KeyID:           keyID,
			Expiration:      time.Now().Add(TestMaxLeaseTime),
			ProtocolVersion: lastProtocolVersion,
		}

		if err := CreateLease(ctx, tx, lease3); err != nil {
			return fmt.Errorf("could not add new lease: %w", err)
		}

		leases, err := FindAllLeasesByRepositoryAndOverlappingPath(ctx, tx, repo, path1)
		if err != nil {
			return fmt.Errorf("could not retrieve leases: %w", err)
		}

		if len(leases) != 3 {
			return fmt.Errorf("conflicting leases not found: %w", err)
		}

		return nil
	})

}

func TestLeaseExpired(t *testing.T) {
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
	repo := "test.repo.org"
	path := "path/one"

	withTx(context.Background(), db.SQL, t, func(ctx context.Context, tx *sql.Tx) error {
		token1 := NewLeaseToken()

		lease1 := Lease{
			Token:           token1,
			Repository:      repo,
			Path:            path,
			KeyID:           keyID,
			Expiration:      time.Now().Add(shortLeaseTime),
			ProtocolVersion: lastProtocolVersion,
		}
		if err := CreateLease(ctx, tx, lease1); err != nil {
			return fmt.Errorf("could not add new lease: %w", err)
		}

		time.Sleep(2 * shortLeaseTime)

		token2 := NewLeaseToken()
		lease2 := Lease{
			Token:           token2,
			Repository:      repo,
			Path:            path,
			KeyID:           keyID,
			Expiration:      time.Now().Add(shortLeaseTime),
			ProtocolVersion: lastProtocolVersion,
		}

		if err := CreateLease(ctx, tx, lease2); err != nil {
			return fmt.Errorf("could not add new lease in place of expired one")
		}

		return nil
	})
}
