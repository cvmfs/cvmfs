package backend

import (
	"context"
	"fmt"
	"sync"
	"time"

	gw "github.com/cvmfs/gateway/internal/gateway"
)

var leaseMutex sync.Mutex

// LeaseDTO is the lease information returned to the HTTP frontend
type LeaseDTO struct {
	KeyID     string `json:"key_id,omitempty"`
	LeasePath string `json:"path,omitempty"`
	Expires   string `json:"expires,omitempty"`
	Hostname  string `json:"hostname,omitempty"`
}

// NewLease for the specified path, using keyID
func (s *Services) NewLease(ctx context.Context, keyID, leasePath, hostname string, protocolVersion int) (string, error) {
	leaseMutex.Lock()
	defer leaseMutex.Unlock()

	t0 := time.Now()

	outcome := "success"
	defer logAction(ctx, "new_lease", &outcome, t0)

	repo, path, err := gw.SplitLeasePath(leasePath)
	if err != nil {
		outcome = err.Error()
		return "", err
	}

	tx, err := s.DB.SQL.BeginTx(ctx, nil)
	if err != nil {
		return "", fmt.Errorf("could not begin transaction: %w", err)
	}
	defer tx.Rollback()

	repoConfig, err := s.GetRepo(ctx, repo)
	if err != nil {
		return "", fmt.Errorf("could not retrieve repository information: %w", err)
	}
	if repoConfig == nil {
		return "", fmt.Errorf("repository not found: %s", repo)
	}
	if !repoConfig.Enabled {
		return "", ErrRepoDisabled
	}

	// Check if keyID is allowed to request a lease in the repository
	// at the specified subpath
	if err := s.Access.Check(keyID, path, repo); err != nil {
		outcome = err.Error()
		return "", err
	}

	leases, err := FindAllLeasesByRepositoryAndOverlappingPath(ctx, tx, repo, path)
	if err != nil {
		return "", err
	}

	for _, lease := range leases {
		timeLeft := time.Until(lease.Expiration)
		if timeLeft > 0 {
			err := PathBusyError{timeLeft}
			outcome = err.Error()
			return "", err
		}
	}

	// Delete expired leases
	if err := DeleteAllExpiredLeases(ctx, tx); err != nil {
		outcome = err.Error()
		return "", err
	}

	// Generate a new token for the lease
	lease := Lease{
		Token:           NewLeaseToken(),
		Repository:      repo,
		Path:            path,
		KeyID:           keyID,
		Expiration:      time.Now().Add(s.Config.MaxLeaseTime),
		ProtocolVersion: protocolVersion,
		Hostname:        hostname,
	}

	if err := CreateLease(ctx, tx, lease); err != nil {
		outcome = err.Error()
		return "", err
	}

	// the StatsMgr does not handle the case in which a lease expires.
	// However, if a lease expires, we should not upload it's statistics.
	// If the LeaseMgr successfully create a new lease,
	// then, the lease path must be free.
	// We remove it, no matter what.
	// We don't check the error because it return an error if the lease does not exist, the standard case.
	s.StatsMgr.PopLease(lease.CombinedLeasePath())

	if err := s.StatsMgr.CreateLease(lease.CombinedLeasePath()); err != nil {
		outcome = err.Error()
		return "", err
	}

	if err := tx.Commit(); err != nil {
		return "", fmt.Errorf("could not commit transaction: %w", err)
	}

	outcome = fmt.Sprintf("success: %v", lease.Token)
	return lease.Token, nil
}

// GetLeases returns all active and valid leases
func (s *Services) GetLeases(ctx context.Context) (map[string]LeaseDTO, error) {
	leaseMutex.Lock()
	defer leaseMutex.Unlock()
	t0 := time.Now()

	outcome := "success"
	defer logAction(ctx, "get_leases", &outcome, t0)

	tx, err := s.DB.SQL.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("could not begin transaction: %w", err)
	}
	defer tx.Rollback()

	leases, err := FindAllActiveLeases(ctx, tx)
	if err != nil {
		outcome = err.Error()
		return nil, err
	}
	ret := make(map[string]LeaseDTO)
	for _, l := range leases {
		leasePath := l.Repository + l.Path
		ret[leasePath] = LeaseDTO{KeyID: l.KeyID, LeasePath: leasePath, Expires: l.Expiration.String(), Hostname: l.Hostname}
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("could not commit transaction: %w", err)
	}

	return ret, nil
}

// GetLease returns the lease associated with a token
func (s *Services) GetLease(ctx context.Context, token string) (*LeaseDTO, error) {
	leaseMutex.Lock()
	defer leaseMutex.Unlock()
	t0 := time.Now()

	outcome := "success"
	defer logAction(ctx, "get_lease", &outcome, t0)

	tx, err := s.DB.SQL.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("could not begin transaction: %w", err)
	}
	defer tx.Rollback()

	lease, err := FindLeaseByToken(ctx, tx, token)
	if err != nil {
		outcome = err.Error()
		return nil, err
	}

	if lease == nil || lease.Expiration.Before(time.Now()) {
		err := InvalidLeaseError{}
		outcome = err.Error()
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("could not commit transaction: %w", err)
	}

	ret := &LeaseDTO{
		KeyID:     lease.KeyID,
		LeasePath: lease.CombinedLeasePath(),
		Expires:   lease.Expiration.String(),
		Hostname:  lease.Hostname,
	}
	return ret, nil
}

// CancelLeases cancels all the active leases below a repository path
func (s *Services) CancelLeases(ctx context.Context, repoPath string) error {
	leaseMutex.Lock()
	defer leaseMutex.Unlock()
	t0 := time.Now()

	outcome := "success"
	defer logAction(ctx, "cancel_leases", &outcome, t0)

	tx, err := s.DB.SQL.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("could not begin transaction: %w", err)
	}
	defer tx.Rollback()

	repo, path, err := gw.SplitLeasePath(repoPath)
	if err != nil {
		outcome = err.Error()
		return err
	}

	if err := DeleteAllLeasesByRepositoryAndPathPrefix(ctx, tx, repo, path); err != nil {
		outcome = err.Error()
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("could not commit transaction: %w", err)
	}

	return nil
}

// CancelLease associated with the token
func (s *Services) CancelLease(ctx context.Context, token string) error {
	leaseMutex.Lock()
	defer leaseMutex.Unlock()
	t0 := time.Now()

	outcome := "success"
	defer logAction(ctx, "cancel_lease", &outcome, t0)

	tx, err := s.DB.SQL.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("could not begin transaction: %w", err)
	}
	defer tx.Rollback()

	lease, err := FindLeaseByToken(ctx, tx, token)
	if err != nil {
		outcome = err.Error()
		return err
	}

	if lease == nil {
		err := InvalidLeaseError{}
		outcome = err.Error()
		return err
	}

	if err := DeleteLeaseByToken(ctx, tx, token); err != nil {
		outcome = err.Error()
		return err
	}

	// We don't check the error - if the statistics are missing, the lease
	// should still be cancelable
	s.StatsMgr.PopLease(lease.CombinedLeasePath())

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("could not commit transaction: %w", err)
	}

	return nil
}

// CommitLease associated with the token (transaction commit)
func (s *Services) CommitLease(ctx context.Context, token, oldRootHash, newRootHash string, tag gw.RepositoryTag) (uint64, error) {
	leaseMutex.Lock()
	defer leaseMutex.Unlock()
	t0 := time.Now()

	outcome := "success"
	defer logAction(ctx, "commit_lease", &outcome, t0)

	tx, err := s.DB.SQL.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("could not begin transaction: %w", err)
	}
	defer tx.Rollback()

	lease, err := FindLeaseByToken(ctx, tx, token)
	if err != nil {
		outcome = err.Error()
		return 0, err
	}

	if lease == nil || lease.Expiration.Before(time.Now()) {
		err := InvalidLeaseError{}
		outcome = err.Error()
		return 0, err
	}

	var finalRev uint64
	if err := s.DB.WithLock(ctx, lease.Repository, func() error {
		var err error
		leasePath := lease.CombinedLeasePath()
		finalRev, err = s.Pool.CommitLease(ctx, leasePath, oldRootHash, newRootHash, tag)
		return err
	}); err != nil {
		outcome = err.Error()
		return 0, err
	}

	go func() {
		plotsErr := s.StatsMgr.UploadStatsPlots(lease.Repository)
		if plotsErr != nil {
			gw.LogC(ctx, "actions", gw.LogError).Msgf(plotsErr.Error())
		}
	}()

	if err := DeleteLeaseByToken(ctx, tx, token); err != nil {
		outcome = err.Error()
		return finalRev, err
	}

	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("could not commit transaction: %w", err)
	}

	return finalRev, nil
}
