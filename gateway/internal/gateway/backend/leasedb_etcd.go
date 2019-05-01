package backend

import (
	"context"

	gw "github.com/cvmfs/gateway/internal/gateway"
)

// EtcdLeaseDB is a lease DB backed by an Etcd cluster
type EtcdLeaseDB struct {
}

// OpenEtcdLeaseDB creates a new etcd-backed lease DB
func OpenEtcdLeaseDB(endpoints []string) (*EtcdLeaseDB, error) {
	gw.Log("leasedb", gw.LogError).
		Msg("Etcd-backed lease DB not yet implemented")
	return nil, nil
}

// Close the lease database
func (db *EtcdLeaseDB) Close() error {
	return nil
}

// NewLease attemps to acquire a new lease for the given path
func (db *EtcdLeaseDB) NewLease(
	ctx context.Context, keyID, leasePath string, protocolVersion int, token LeaseToken) error {
	return nil
}

// GetLeases returns a list of all active leases
func (db *EtcdLeaseDB) GetLeases(ctx context.Context) (map[string]Lease, error) {
	return map[string]Lease{}, nil
}

// GetLease returns the lease for a given token string
func (db *EtcdLeaseDB) GetLease(ctx context.Context, tokenStr string) (string, *Lease, error) {
	return "", nil, nil
}

// CancelLeases cancels all active leases
func (db *EtcdLeaseDB) CancelLeases(ctx context.Context) error {
	return nil
}

// CancelLease cancels the lease for a token string
func (db *EtcdLeaseDB) CancelLease(ctx context.Context, tokenStr string) error {
	return nil
}

// WithLock runs the given task while holding a commit lock for the repository
func (db *EtcdLeaseDB) WithLock(ctx context.Context, repository string, task func() error) error {
	return nil
}
