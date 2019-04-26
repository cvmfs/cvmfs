package backend

import (
	gw "github.com/cvmfs/gateway/internal/gateway"
	"github.com/cvmfs/gateway/internal/gateway/receiver"
	"github.com/pkg/errors"
)

// Services is a container for the various
// backend services
type Services struct {
	Access AccessConfig
	Leases LeaseDB
	Pool   *receiver.Pool
	Config gw.Config
}

// StartBackend initializes the various backend services
func StartBackend(cfg *gw.Config) (*Services, error) {
	ac, err := NewAccessConfig(cfg.AccessConfigFile)
	if err != nil {
		return nil, errors.Wrap(
			err, "loading repository access configuration failed")
	}

	leaseDBType := "embedded"
	if cfg.UseEtcd {
		leaseDBType = "etcd"
	}
	ldb, err := OpenLeaseDB(leaseDBType, cfg)
	if err != nil {
		return nil, errors.Wrap(err, "could not create lease DB")
	}

	pool, err := receiver.StartPool(cfg.ReceiverPath, cfg.NumReceivers, cfg.MockReceiver)
	if err != nil {
		return nil, errors.Wrap(err, "could not start receiver pool")
	}

	return &Services{Access: *ac, Leases: ldb, Pool: pool, Config: *cfg}, nil
}

// Stop all the backend services
func (s *Services) Stop() error {
	if err := s.Leases.Close(); err != nil {
		return errors.Wrap(err, "could not close lease database")
	}
	return nil
}
