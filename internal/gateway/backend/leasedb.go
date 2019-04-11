package backend

import (
	"fmt"

	gw "github.com/cvmfs/gateway/internal/gateway"
)

// LeaseDB provides a consistent store for repository leases
type LeaseDB interface {
}

// NewLeaseDB creates a new LeaseDB object of the specified type
// (either "embedded" or "etcd").
func NewLeaseDB(dbType string, config *gw.Config) (LeaseDB, error) {
	switch dbType {
	case "embedded":
		return NewEmbeddedLeaseDB(config)
	case "etcd":
		return NewEtcdLeaseDB(config)
	default:
		return nil, fmt.Errorf("unknown lease DB type: %v", dbType)
	}
}
