package backend

import (
	"time"

	gw "github.com/cvmfs/gateway/internal/gateway"
)

// EtcdLeaseDB is a lease DB backed by an Etcd cluster
type EtcdLeaseDB struct {
}

// NewEtcdLeaseDB creates a new etcd-backed lease DB
func NewEtcdLeaseDB(endpoints []string, maxLeaseTime time.Duration) (*EtcdLeaseDB, error) {
	gw.Log.Error().Msg("Etcd-backed lease DB not yet implemented")
	return nil, nil
}

// Close the lease database
func (db *EtcdLeaseDB) Close() error {
	return nil
}

// NewLease attemps to acquire a new lease for the given path
func (db *EtcdLeaseDB) NewLease(
	keyID, leasePath string, token LeaseToken) error {
	return nil
}
