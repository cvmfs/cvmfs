package backend

import (
	"bytes"
	"encoding/gob"
	"time"

	"github.com/pkg/errors"
)

// EmbeddedLeaseDB is a LeaseDB backed by BoltDB
type EmbeddedLeaseDB struct {
	workDir      string
	maxLeaseTime time.Duration
	data         map[string][]byte
}

type lease struct {
	KeyID string
	Token LeaseToken
}

// NewEmbeddedLeaseDB creates a new embedded lease DB
func NewEmbeddedLeaseDB(workDir string, maxLeaseTime time.Duration) (*EmbeddedLeaseDB, error) {
	return &EmbeddedLeaseDB{workDir, maxLeaseTime, make(map[string][]byte)}, nil
}

// NewLease attemps to acquire a new lease for the given path
func (db *EmbeddedLeaseDB) NewLease(keyID, leasePath string, token LeaseToken) error {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)

	if err := enc.Encode(lease{KeyID: keyID, Token: token}); err != nil {
		return errors.Wrap(err, "serialization error")
	}

	var existing struct {
		path  string
		lease lease
	}
	for k, v := range db.data {
		if CheckPathOverlap(k, leasePath) {
			existing.path = k
			dec := gob.NewDecoder(bytes.NewReader(v))
			if err := dec.Decode(&existing.lease); err != nil {
				return errors.Wrap(err, "deserialization error")
			}
			break
		}
	}
	if existing.path != "" {
		timeLeft := time.Now().Sub(existing.lease.Token.Expiration)
		if timeLeft <= 0 {
			delete(db.data, existing.lease.Token.TokenStr)
			delete(db.data, existing.path)
			db.data[leasePath] = buf.Bytes()
			db.data[token.TokenStr] = []byte(leasePath)
		} else {
			return PathBusyError{timeLeft}
		}
	} else {
		db.data[leasePath] = buf.Bytes()
		db.data[token.TokenStr] = []byte(leasePath)
	}

	return nil
}
