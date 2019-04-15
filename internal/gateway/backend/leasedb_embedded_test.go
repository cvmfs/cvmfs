package backend

import (
	"io/ioutil"
	"testing"
	"time"
)

const (
	maxLeaseTime time.Duration = 100 * time.Second
)

func TestEmbeddedLeaseDBOpen(t *testing.T) {
	tmp, err := ioutil.TempDir("", "testleasedb")
	if err != nil {
		t.Fatalf("could not create temp dir for test case")
	}

	db, err := NewEmbeddedLeaseDB(tmp, maxLeaseTime)
	if err != nil {
		t.Fatalf("could not create database: %v", err)
	}
	defer db.Close()
}

func TestEmbeddedLeaseDBCRUD(t *testing.T) {
	tmp, err := ioutil.TempDir("", "testleasedb")
	if err != nil {
		t.Fatalf("could not create temp dir for test case")
	}

	db, err := NewEmbeddedLeaseDB(tmp, maxLeaseTime)
	if err != nil {
		t.Fatalf("could not create database: %v", err)
	}
	defer db.Close()

	t.Run("new lease", func(t *testing.T) {
		keyID := "key1"
		leasePath := "test.repo.org/path/one"
		token1, err := NewLeaseToken(leasePath, maxLeaseTime)
		if err != nil {
			t.Fatalf("could not generate session token: %v", err)
		}

		if err := db.NewLease(keyID, leasePath, *token1); err != nil {
			t.Fatalf("could not add new lease: %v", err)
		}
	})
}
