package backend

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"time"

	gw "github.com/cvmfs/gateway/internal/gateway"
	"github.com/pkg/errors"
)

// PathBusyError is returned as error value for new lease requests on
// paths which are already leased
type PathBusyError struct {
	remaining time.Duration
}

func (e PathBusyError) Error() string {
	return fmt.Sprintf("path busy, remaining = %vs", e.remaining.Seconds())
}

// Remaining number of seconds on the existing lease
func (e *PathBusyError) Remaining() time.Duration {
	return e.remaining
}

// LeaseExpiredError is returned by the GetLeaseXXXX methods in case a
// lease exists for the specified path but has already expired
type LeaseExpiredError struct {
}

func (e LeaseExpiredError) Error() string {
	return fmt.Sprintf("lease expired")
}

// InvalidLeaseError is returned by the GetLeaseXXXX methods in case a
// lease does not exist for the specified path
type InvalidLeaseError struct {
}

func (e InvalidLeaseError) Error() string {
	return fmt.Sprintf("invalid lease")
}

// Lease describes an exclusive lease to a subpath inside the repository:
// keyID and token ()
type Lease struct {
	KeyID string
	Token LeaseToken
}

// Serialize the lease to a byte buffer
func (e *Lease) Serialize() ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)

	if err := enc.Encode(*e); err != nil {
		return nil, errors.Wrap(err, "serialization error")
	}

	return buf.Bytes(), nil
}

// DeserializeLease from a byte buffer
func DeserializeLease(buf []byte) (*Lease, error) {
	lease := Lease{}
	dec := gob.NewDecoder(bytes.NewReader(buf))
	if err := dec.Decode(&lease); err != nil {
		return nil, errors.Wrap(err, "deserialization error")
	}

	return &lease, nil
}

// LeaseDB provides a consistent store for repository leases
type LeaseDB interface {
	Close() error
	NewLease(keyID, leasePath string, token LeaseToken) error
	GetLeases() (map[string]Lease, error)
	GetLeaseForPath(leasePath string) (*Lease, error)
	GetLeaseForToken(tokenStr string) (string, *Lease, error)
	CancelLeases() error
	CancelLeaseForPath(leasePath string) error
	CancelLeaseForToken(tokenStr string) error
}

// NewLeaseDB creates a new LeaseDB object of the specified type
// (either "embedded" or "etcd").
func NewLeaseDB(dbType string, config *gw.Config) (LeaseDB, error) {
	maxLeaseTime := time.Duration(config.MaxLeaseTime)
	switch dbType {
	case "embedded":
		return NewEmbeddedLeaseDB(config.WorkDir, maxLeaseTime)
	case "etcd":
		return NewEtcdLeaseDB(config.EtcdEndpoints, maxLeaseTime)
	default:
		return nil, fmt.Errorf("unknown lease DB type: %v", dbType)
	}
}
