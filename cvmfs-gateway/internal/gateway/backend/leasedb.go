package backend

import (
	"bytes"
	"context"
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

// InvalidLeaseError is returned by the GetLeaseXXXX methods in case a
// lease does not exist for the specified path
type InvalidLeaseError struct {
}

func (e InvalidLeaseError) Error() string {
	return fmt.Sprintf("invalid lease")
}

// ErrRepoDisabled signals that a new lease cannot be acquired due to the repository
// being disabled
var ErrRepoDisabled = fmt.Errorf("repo_disabled")

// Lease describes an exclusive lease to a subpath inside the repository:
// keyID and token ()
type Lease struct {
	KeyID           string
	ProtocolVersion int
	Token           LeaseToken
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
	NewLease(ctx context.Context, keyID, leasePath string, protocolVersion int, token LeaseToken) error
	GetLeases(ctx context.Context) (map[string]Lease, error)
	GetLease(ctx context.Context, tokenStr string) (string, *Lease, error)
	CancelLeases(ctx context.Context, repoPath string) error
	CancelLease(ctx context.Context, tokenStr string) error
	WithLock(ctx context.Context, name string, task func() error) error
	SetRepositoryEnabled(ctx context.Context, repository string, enable bool) error
	GetRepositoryEnabled(ctx context.Context, repository string) bool
}

// OpenLeaseDB opens or creats a new LeaseDB object of the specified type
// (either "embedded" or "etcd").
func OpenLeaseDB(dbType string, config *gw.Config) (LeaseDB, error) {
	switch dbType {
	case "boltdb":
		return OpenBoltLeaseDB(config.WorkDir)
	case "sqlite":
		return OpenSqliteLeaseDB(config.WorkDir)
	case "etcd":
		return OpenEtcdLeaseDB(config.EtcdEndpoints)
	default:
		return nil, fmt.Errorf("unknown lease DB type: %v", dbType)
	}
}
