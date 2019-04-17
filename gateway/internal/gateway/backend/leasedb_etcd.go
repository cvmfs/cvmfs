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

// GetLeaseForPath returns the lease for a given path
func (db *EtcdLeaseDB) GetLeaseForPath(leasePath string) (*Lease, error) {
	return nil, nil
}

// GetLeaseForToken returns the lease for a given token string
func (db *EtcdLeaseDB) GetLeaseForToken(tokenStr string) (string, *Lease, error) {
	return "", nil, nil
}

// CancelLeases cancels all active leases
func (db *EtcdLeaseDB) CancelLeases() error {
	return nil
}

// CancelLeaseForPath cancels the leases for a given path
func (db *EtcdLeaseDB) CancelLeaseForPath(leasePath string) error {
	return nil
}

// CancelLeaseForToken cancels the lease for a token string
func (db *EtcdLeaseDB) CancelLeaseForToken(tokenStr string) error {
	return nil
}
