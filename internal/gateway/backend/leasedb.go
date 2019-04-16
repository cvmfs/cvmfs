package backend

import (
	"fmt"
	"time"

	gw "github.com/cvmfs/gateway/internal/gateway"
)

// Lease describes an exclusive lease to a subpath inside the repository:
// keyID and token ()
type Lease struct {
	KeyID string
	Token LeaseToken
}

// PathBusyError is returned as error value for new lease requests on
// paths which are already leased
type PathBusyError struct {
	remaining time.Duration
}

func (e PathBusyError) Error() string {
	return fmt.Sprintf("path busy, remaining = %vs", e.remaining.Seconds())
}

// Remaining number of seconds on the existing lease
func (e *PathBusyError) Remaining() int64 {
	return e.remaining.Nanoseconds() / 1000000000
}

// LeaseDB provides a consistent store for repository leases
type LeaseDB interface {
	Close() error
	NewLease(keyID, leasePath string, token LeaseToken) error
	GetLeases() ([]Lease, error)
	GetLeaseForPath(leasePath string) (*Lease, error)
	GetLeaseForToken(tokenStr string) (*Lease, error)
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
