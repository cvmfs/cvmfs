package backend

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"os"
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
	if err := os.MkdirAll(workDir, 0777); err != nil {
		return nil, errors.Wrap(err, "could not create directory for backing store")
	}
	store, err := bolt.Open(workDir+"/lease.db", 0666, nil)
	if err != nil {
		return nil, errors.Wrap(err, "could not open backing store (BoltDB)")
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

		tokenBucket, err := txn.CreateBucketIfNotExists([]byte("tokens"))
		if err != nil {
			return errors.Wrap(err, "could not create bucket: 'tokens'")
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
				dec := gob.NewDecoder(bytes.NewReader(v))
				if err := dec.Decode(&existing.lease); err != nil {
					return errors.Wrap(err, "deserialization error")
				}
				existing.path = kPath
				break
			}
		}

		// If a conflicting lease exists, examine its expiration time
		if existing.path != "" {
			timeLeft := existing.lease.Token.Expiration.Sub(time.Now())
			if timeLeft > 0 {
				return PathBusyError{timeLeft}
			}

			tokenBucket.Delete([]byte(existing.lease.Token.TokenStr))
			repoBucket.Delete([]byte(existing.path))
			tokenBucket.Put([]byte(token.TokenStr), []byte(leasePath))
			repoBucket.Put([]byte(subPath), buf.Bytes())
		} else {
			tokenBucket.Put([]byte(token.TokenStr), []byte(leasePath))
			repoBucket.Put([]byte(subPath), buf.Bytes())
		}

		return nil
	})
}

// GetLeases returns a list of all active leases
func (db *EmbeddedLeaseDB) GetLeases() (map[string]Lease, error) {
	leases := make(map[string]Lease)
	err := db.store.View(func(txn *bolt.Tx) error {
		if err := txn.ForEach(func(name []byte, b *bolt.Bucket) error {
			bucketName := string(name)
			if bucketName == "tokens" {
				return nil
			}
			if err := b.ForEach(func(k, v []byte) error {
				leasePath := bucketName + "/" + string(k)
				lease := Lease{}
				dec := gob.NewDecoder(bytes.NewReader(v))
				if err := dec.Decode(&lease); err != nil {
					return errors.Wrap(err, "deserialization error")
				}
				leases[leasePath] = lease
				return nil
			}); err != nil {
				return errors.Wrap(
					err, fmt.Sprintf("iteration error in bucket: %v", bucketName))
			}
			return nil
		}); err != nil {
			return errors.Wrap(err, "iteration error over repository buckets")
		}

		return nil
	})
	return leases, err
}

// GetLeaseForPath returns the lease for a given path
func (db *EmbeddedLeaseDB) GetLeaseForPath(leasePath string) (*Lease, error) {
	lease := Lease{}
	err := db.store.View(func(txn *bolt.Tx) error {
		repoName, subPath, err := SplitLeasePath(leasePath)
		if err != nil {
			return errors.Wrap(err, "invalid lease path")
		}
		bucket := txn.Bucket([]byte(repoName))
		if bucket == nil {
			return fmt.Errorf("invalid repo name: %v", repoName)
		}
		v := bucket.Get([]byte(subPath))
		if v == nil {
			return InvalidLeaseError{}
		}
		dec := gob.NewDecoder(bytes.NewReader(v))
		if err := dec.Decode(&lease); err != nil {
			return errors.Wrap(err, "deserialization error")
		}
		if lease.Token.Expiration.Sub(time.Now()) <= 0 {
			return LeaseExpiredError{}
		}
		return nil
	})
	return &lease, err
}

// GetLeaseForToken returns the lease for a given token string
func (db *EmbeddedLeaseDB) GetLeaseForToken(tokenStr string) (*Lease, error) {
	lease := Lease{}
	err := db.store.View(func(txn *bolt.Tx) error {
		tokens := txn.Bucket([]byte("tokens"))
		if tokens == nil {
			return fmt.Errorf("missing 'tokens' bucket")
		}
		leasePath := tokens.Get([]byte(tokenStr))
		if leasePath == nil {
			return InvalidLeaseError{}
		}

		repoName, subPath, err := SplitLeasePath(string(leasePath))
		if err != nil {
			return errors.Wrap(err, "invalid lease path")
		}
		bucket := txn.Bucket([]byte(repoName))
		if bucket == nil {
			return fmt.Errorf("invalid repo name: %v", repoName)
		}
		v := bucket.Get([]byte(subPath))
		dec := gob.NewDecoder(bytes.NewReader(v))
		if err := dec.Decode(&lease); err != nil {
			return errors.Wrap(err, "deserialization error (lease)")
		}
		return nil
	})
	return &lease, err
}

// CancelLeases cancels all active leases
func (db *EmbeddedLeaseDB) CancelLeases() error {
	return db.store.Update(func(txn *bolt.Tx) error {
		txn.ForEach(func(name []byte, b *bolt.Bucket) error {
			txn.DeleteBucket(name)
			return nil
		})
		return nil
	})
}

// CancelLeaseForPath cancels the leases for a given path
func (db *EmbeddedLeaseDB) CancelLeaseForPath(leasePath string) error {
	return db.store.Update(func(txn *bolt.Tx) error {
		repoName, subPath, err := SplitLeasePath(leasePath)
		if err != nil {
			return errors.Wrap(err, "invalid lease path")
		}

		tokens := txn.Bucket([]byte("tokens"))
		if tokens == nil {
			return fmt.Errorf("missing 'tokens' bucket")
		}
		bucket := txn.Bucket([]byte(repoName))
		if bucket == nil {
			return fmt.Errorf("invalid repo name: %v", repoName)
		}

		v := bucket.Get([]byte(subPath))
		if v == nil {
			return InvalidLeaseError{}
		}
		lease := Lease{}
		dec := gob.NewDecoder(bytes.NewReader(v))
		if err := dec.Decode(&lease); err != nil {
			return errors.Wrap(err, "deserialization error")
		}

		bucket.Delete([]byte(subPath))
		tokens.Delete([]byte(lease.Token.TokenStr))

		return nil
	})
}

// CancelLeaseForToken cancels the lease for a token string
func (db *EmbeddedLeaseDB) CancelLeaseForToken(tokenStr string) error {
	return db.store.Update(func(txn *bolt.Tx) error {
		tokens := txn.Bucket([]byte("tokens"))
		if tokens == nil {
			return fmt.Errorf("missing 'tokens' bucket")
		}
		leasePath := tokens.Get([]byte(tokenStr))
		if leasePath == nil {
			return InvalidLeaseError{}
		}

		repoName, subPath, err := SplitLeasePath(string(leasePath))
		if err != nil {
			return errors.Wrap(err, "invalid lease path")
		}
		bucket := txn.Bucket([]byte(repoName))
		if bucket == nil {
			return fmt.Errorf("invalid repo name: %v", repoName)
		}

		bucket.Delete([]byte(subPath))
		tokens.Delete([]byte(tokenStr))

		return nil
	})
}
