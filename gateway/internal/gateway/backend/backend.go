package backend

import (
	"context"
	"fmt"
	"io"

	gw "github.com/cvmfs/gateway/internal/gateway"
	"github.com/cvmfs/gateway/internal/gateway/receiver"
	stats "github.com/cvmfs/gateway/internal/gateway/statistics"
)

// Services is a container for the various
// backend services
type Services struct {
	Access        AccessConfig
	Leases        *LeaseDB
	Pool          *receiver.Pool
	Notifications *NotificationSystem
	Config        gw.Config
	StatsMgr      *stats.StatisticsMgr
}

// ActionController contains the various actions that can be performed with the backend
type ActionController interface {
	GetKey(ctx context.Context, keyID string) *KeyConfig
	GetRepo(ctx context.Context, repoName string) *RepositoryConfig
	GetRepos(ctx context.Context) map[string]RepositoryConfig
	SetRepoEnabled(ctx context.Context, repository string, enabled bool) error
	NewLease(ctx context.Context, keyID, leasePath string, protocolVersion int) (string, error)
	GetLeases(ctx context.Context) (map[string]LeaseReturn, error)
	GetLease(ctx context.Context, tokenStr string) (*LeaseReturn, error)
	CancelLeases(ctx context.Context, repoPath string) error
	CancelLease(ctx context.Context, tokenStr string) error
	CommitLease(ctx context.Context, tokenStr, oldRootHash, newRootHash string, tag gw.RepositoryTag) (uint64, error)
	SubmitPayload(ctx context.Context, token string, payload io.Reader, digest string, headerSize int) error
	RunGC(ctx context.Context, options GCOptions) (string, error)
	PublishManifest(ctx context.Context, repository string, message NotificationMessage)
	SubscribeToNotifications(ctx context.Context, repository string) SubscriberHandle
	UnsubscribeFromNotifications(ctx context.Context, repository string, handle SubscriberHandle) error
}

// GetKey returns the key configuration associated with a key ID
func (s *Services) GetKey(ctx context.Context, keyID string) *KeyConfig {
	return s.Access.GetKeyConfig(keyID)
}

// StartBackend initializes the various backend services
func StartBackend(cfg *gw.Config) (*Services, error) {
	ac, err := NewAccessConfig(cfg.AccessConfigFile)
	if err != nil {
		return nil, fmt.Errorf("loading repository access configuration failed: %w", err)
	}

	ldb, err := OpenLeaseDB(cfg.LeaseDB, cfg)
	if err != nil {
		return nil, fmt.Errorf("could not create lease DB: %w", err)
	}

	smgr := stats.NewStatisticsMgr()

	pool, err := receiver.StartPool(cfg.ReceiverPath, cfg.NumReceivers, cfg.MockReceiver, smgr)
	if err != nil {
		return nil, fmt.Errorf("could not start receiver pool: %w", err)
	}

	ns, err := NewNotificationSystem(cfg.WorkDir)
	if err != nil {
		return nil, fmt.Errorf("could not initialize notification system: %w", err)
	}

	return &Services{Access: *ac, Leases: ldb, Pool: pool, Notifications: ns, Config: *cfg, StatsMgr: smgr}, nil
}

// Stop all the backend services
func (s *Services) Stop() error {
	if err := s.Leases.Close(); err != nil {
		return fmt.Errorf("could not close lease database: %w", err)
	}
	return nil
}
