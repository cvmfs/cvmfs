package backend

import (
	"context"
	"io"

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

// ActionController contains the various actions that can be performed with the backend
type ActionController interface {
	GetSecret(keyID string) string
	GetRepo(repoName string) RepositoryConfig
	GetRepos() map[string]RepositoryConfig
	NewLease(ctx context.Context, keyID, leasePath string, protocolVersion int) (string, error)
	GetLeases(ctx context.Context) (map[string]LeaseReturn, error)
	GetLease(ctx context.Context, tokenStr string) (*LeaseReturn, error)
	CancelLease(ctx context.Context, tokenStr string) error
	CommitLease(ctx context.Context, tokenStr, oldRootHash, newRootHash string, tag gw.RepositoryTag) error
	SubmitPayload(ctx context.Context, token string, payload io.Reader, digest string, headerSize int) error
}

// GetSecret associated with a key ID
func (s *Services) GetSecret(keyID string) string {
	return s.Access.GetKeyConfig(keyID).Secret
}

// StartBackend initializes the various backend services
func StartBackend(cfg *gw.Config) (*Services, error) {
	ac, err := NewAccessConfig(cfg.AccessConfigFile)
	if err != nil {
		return nil, errors.Wrap(
			err, "loading repository access configuration failed")
	}

	ldb, err := OpenLeaseDB(cfg.LeaseDB, cfg)
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
