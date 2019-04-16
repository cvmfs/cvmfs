package backend

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"time"

	"github.com/pkg/errors"
	bolt "go.etcd.io/bbolt"
)

// EmbeddedLeaseDB is a LeaseDB backed by BoltDB
type EmbeddedLeaseDB struct {
	store        *bolt.DB
	maxLeaseTime time.Duration
}

// NewEmbeddedLeaseDB creates a new embedded lease DB
func NewEmbeddedLeaseDB(workDir string, maxLeaseTime time.Duration) (*EmbeddedLeaseDB, error) {
	store, err := bolt.Open(workDir+"/lease.db", 0666, nil)
	if err != nil {
		return nil, errors.Wrap(err, "could not open backing store (BoltDB)")
	}
	if err := store.Update(func(txn *bolt.Tx) error {
		if _, err := txn.CreateBucketIfNotExists([]byte("tokens")); err != nil {
			return errors.Wrap(err, "could not create bucket: 'tokens'")
		}
		return nil
	}); err != nil {
		return nil, err
	}

	return &EmbeddedLeaseDB{store, maxLeaseTime}, nil
}

// Close the lease database
func (db *EmbeddedLeaseDB) Close() error {
	return db.store.Close()
}

// NewLease attemps to acquire a new lease for the given path
func (db *EmbeddedLeaseDB) NewLease(keyID, leasePath string, token LeaseToken) error {
	return db.store.Update(func(txn *bolt.Tx) error {
		repoName, subPath, err := SplitLeasePath(leasePath)
		if err != nil {
			return errors.Wrap(err, "invalid lease path")
		}

		var buf bytes.Buffer
		enc := gob.NewEncoder(&buf)

		if err := enc.Encode(Lease{KeyID: keyID, Token: token}); err != nil {
			return errors.Wrap(err, "serialization error")
		}

		tokenBucket := txn.Bucket([]byte("tokens"))
		if tokenBucket == nil {
			return fmt.Errorf("missing bucket: 'tokens'")
		}

		repoBucket, err := txn.CreateBucketIfNotExists([]byte(repoName))
		if err != nil {
			return errors.Wrap(err, fmt.Sprintf("could not create bucket: '%v'", repoName))
		}

		// Iterate through the key of the bucket corresponding to the repository
		// and find any conflicting lease (there can be at most 1)
		var existing struct {
			path  string
			lease Lease
		}

		cursor := repoBucket.Cursor()
		for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
			kPath := string(k)
			if CheckPathOverlap(kPath, subPath) {
				existing.path = kPath
				dec := gob.NewDecoder(bytes.NewReader(v))
				if err := dec.Decode(&existing.lease); err != nil {
					return errors.Wrap(err, "deserialization error")
				}
				break
			}
		}

		// If a conflicting lease exists, examine its expiration time
		if existing.path != "" {
			timeLeft := time.Now().Sub(existing.lease.Token.Expiration)
			if timeLeft <= 0 {
				tokenBucket.Delete([]byte(existing.lease.Token.TokenStr))
				repoBucket.Delete([]byte(existing.path))
				tokenBucket.Put([]byte(token.TokenStr), []byte(subPath))
				repoBucket.Put([]byte(subPath), buf.Bytes())
			} else {
				return PathBusyError{timeLeft}
			}
		} else {
			tokenBucket.Put([]byte(token.TokenStr), []byte(subPath))
			repoBucket.Put([]byte(subPath), buf.Bytes())
		}

		return nil
	})
}

// GetLeases returns a list of all active leases
func (db *EmbeddedLeaseDB) GetLeases() ([]Lease, error) {
	return []Lease{}, nil
}

// GetLeaseForPath returns the lease for a given path
func (db *EmbeddedLeaseDB) GetLeaseForPath(leasePath string) (*Lease, error) {
	return nil, nil
}

// GetLeaseForToken returns the lease for a given token string
func (db *EmbeddedLeaseDB) GetLeaseForToken(tokenStr string) (*Lease, error) {
	return nil, nil
}

// CancelLeases cancels all active leases
func (db *EmbeddedLeaseDB) CancelLeases() error {
	return nil
}

// CancelLeaseForPath cancels the leases for a given path
func (db *EmbeddedLeaseDB) CancelLeaseForPath(leasePath string) error {
	return nil
}

// CancelLeaseForToken cancels the lease for a token string
func (db *EmbeddedLeaseDB) CancelLeaseForToken(tokenStr string) error {
	return nil
}
