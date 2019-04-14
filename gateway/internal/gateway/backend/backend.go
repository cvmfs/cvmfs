package backend

import (
	gw "github.com/cvmfs/gateway/internal/gateway"
	"github.com/pkg/errors"
)

// Services is a container for the various
// backend services
type Services struct {
	Access AccessConfig
	Leases LeaseDB
}

// Start initializes the various backend services
func Start(cfg *gw.Config) (*Services, error) {
	ac, err := NewAccessConfig(cfg.AccessConfigFile)
	if err != nil {
		return nil, errors.Wrap(
			err, "loading repository access configuration failed")
	}

	leaseDBType := "embedded"
	if cfg.UseEtcd {
		leaseDBType = "etcd"
	}
	ldb, err := NewLeaseDB(leaseDBType, cfg)
	if err != nil {
		return nil, errors.Wrap(
			err, "could not create lease DB")
	}

	return &Services{Access: *ac, Leases: ldb}, nil
}

// RequestNewLease for the specified path, using keyID
func (s *Services) RequestNewLease(keyID, leasePath string) (string, error) {
	repoName, subPath, err := SplitLeasePath(leasePath)
	if err != nil {
		return "", errors.Wrap(err, "could not parse lease path")
	}

	if err := s.Access.Check(keyID, subPath, repoName); err != nil {
		return "", err
	}

	return "", nil
}
