package backend

import (
	gw "github.com/cvmfs/gateway/internal/gateway"
)

// EtcdLeaseDB is a lease DB backed by an Etcd cluster
type EtcdLeaseDB struct {
}

// NewEtcdLeaseDB creates a new etcd-backed lease DB
func NewEtcdLeaseDB(endpoints []string) (*EtcdLeaseDB, error) {
	gw.Log.Error().Msg("Etcd-backed lease DB not yet implemented")
	return nil, nil
}

// NewLease attemps to acquire a new lease for the given path
func (db *EtcdLeaseDB) NewLease(keyID, leasePath string) (string, error) {
	return "", nil
}
