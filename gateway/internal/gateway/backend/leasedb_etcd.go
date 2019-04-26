package backend

import (
	gw "github.com/cvmfs/gateway/internal/gateway"
)

// EtcdLeaseDB is a lease DB backed by an Etcd cluster
type EtcdLeaseDB struct {
}

// OpenEtcdLeaseDB creates a new etcd-backed lease DB
func OpenEtcdLeaseDB(endpoints []string) (*EtcdLeaseDB, error) {
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

// GetLeases returns a list of all active leases
func (db *EtcdLeaseDB) GetLeases() (map[string]Lease, error) {
	return map[string]Lease{}, nil
}

// GetLease returns the lease for a given token string
func (db *EtcdLeaseDB) GetLease(tokenStr string) (string, *Lease, error) {
	return "", nil, nil
}

// CancelLeases cancels all active leases
func (db *EtcdLeaseDB) CancelLeases() error {
	return nil
}

// CancelLease cancels the lease for a token string
func (db *EtcdLeaseDB) CancelLease(tokenStr string) error {
	return nil
}

// WithLock runs the given task while holding a commit lock for the repository
func (db *EtcdLeaseDB) WithLock(repository string, task func() error) error {
	return nil
}
