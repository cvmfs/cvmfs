package backend

import (
	gw "github.com/cvmfs/gateway/internal/gateway"
)

// EtcdLeaseDB is a lease DB backed by an Etcd cluster
type EtcdLeaseDB struct {
}

// NewEtcdLeaseDB creates a new etcd-backed lease DB
func NewEtcdLeaseDB(config *gw.Config) (*EtcdLeaseDB, error) {
	gw.Log.Error().Msg("Etcd-backed lease DB not yet implemented")
	return nil, nil
}
