package backend

import gw "github.com/cvmfs/gateway/internal/gateway"

// EmbeddedLeaseDB is a LeaseDB backed by BoltDB
type EmbeddedLeaseDB struct {
	workDir string
}

// NewEmbeddedLeaseDB creates a new embedded lease DB
func NewEmbeddedLeaseDB(config *gw.Config) (*EmbeddedLeaseDB, error) {
	return &EmbeddedLeaseDB{""}, nil
}
