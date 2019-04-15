package backend

import (
	"time"

	gw "github.com/cvmfs/gateway/internal/gateway"
	"github.com/pkg/errors"
)

// Services is a container for the various
// backend services
type Services struct {
	Access AccessConfig
	Leases LeaseDB
	Config gw.Config
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
		return nil, errors.Wrap(err, "could not create lease DB")
	}

	return &Services{Access: *ac, Leases: ldb, Config: *cfg}, nil
}

// RequestNewLease for the specified path, using keyID
func (s *Services) RequestNewLease(keyID, leasePath string) (string, error) {
	repoName, subPath, err := SplitLeasePath(leasePath)
	if err != nil {
		return "", errors.Wrap(err, "could not parse lease path")
	}

	// Check if keyID is allowed to request a lease in the repository
	// at the specified subpath
	if err := s.Access.Check(keyID, subPath, repoName); err != nil {
		return "", err
	}

	// Generate a new token for the lease
	token, err := NewLeaseToken(
		leasePath, time.Duration(s.Config.MaxLeaseTime)*time.Second)
	if err != nil {
		return "", errors.Wrap(err, "could not generate session token")
	}

	if err := s.Leases.NewLease(keyID, leasePath, *token); err != nil {
		return "", err
	}

	return token.TokenStr, nil
}
