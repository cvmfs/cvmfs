package backend

// EmbeddedLeaseDB is a LeaseDB backed by BoltDB
type EmbeddedLeaseDB struct {
	workDir string
}

// NewEmbeddedLeaseDB creates a new embedded lease DB
func NewEmbeddedLeaseDB(workDir string) (*EmbeddedLeaseDB, error) {
	return &EmbeddedLeaseDB{workDir}, nil
}

// NewLease attemps to acquire a new lease for the given path
func (db *EmbeddedLeaseDB) NewLease(keyID, leasePath string) (string, error) {
	return "", nil
}
