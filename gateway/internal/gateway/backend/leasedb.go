package backend

import (
	"fmt"
	"time"

	gw "github.com/cvmfs/gateway/internal/gateway"
)

// NewLeaseResult is returned when requesting a new lease. Status is true
// if the new lease was created, or false if there is a conflicting lease
// (with Remaining number of seconds). Token represents the session token
// of the new lease
type NewLeaseResult struct {
	Status      string // "ok" | "path_busy" | "error"
	Token       string
	Remaining   time.Duration
	ErrorReason string
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
	NewLease(keyID, leasePath string) (string, error)
}

// NewLeaseDB creates a new LeaseDB object of the specified type
// (either "embedded" or "etcd").
func NewLeaseDB(dbType string, config *gw.Config) (LeaseDB, error) {
	switch dbType {
	case "embedded":
		return NewEmbeddedLeaseDB(config.WorkDir)
	case "etcd":
		return NewEtcdLeaseDB(config.EtcdEndpoints)
	default:
		return nil, fmt.Errorf("unknown lease DB type: %v", dbType)
	}
}
