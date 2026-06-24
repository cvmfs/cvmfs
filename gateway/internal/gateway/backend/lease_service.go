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

// RefreshLease extends the expiration of an active lease identified by token.
// The new expiration is set to now + extension, where extension defaults to
// Config.LeaseRefreshTime when the caller passes 0 (requestedExtension). The
// extension is capped at Config.MaxLeaseTime. An expired (or non-existent)
// lease cannot be refreshed, as its path may already have been granted to
// another publisher.
func (s *Services) RefreshLease(ctx context.Context, token string, requestedExtension time.Duration) (*LeaseDTO, error) {
	leaseMutex.Lock()
	defer leaseMutex.Unlock()
	t0 := time.Now()

	outcome := "success"
	defer logAction(ctx, "refresh_lease", &outcome, t0)

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

	extension := requestedExtension
	if extension <= 0 {
		extension = s.Config.LeaseRefreshTime
	}
	if extension > s.Config.MaxLeaseTime {
		extension = s.Config.MaxLeaseTime
	}

	newExpiration := time.Now().Add(extension)
	if _, err := UpdateLeaseExpirationByToken(ctx, tx, token, newExpiration); err != nil {
		outcome = err.Error()
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("could not commit transaction: %w", err)
	}

	lease.Expiration = newExpiration
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
	return s.commitLease(ctx, token, oldRootHash, newRootHash, tag, false)
}

// GraftLease commits the lease using the experimental dedicated DirectGraft
// path: the pre-built subtree catalog is grafted into the parent catalog,
// skipping the DiffRec catalog merge.  This is intentionally kept separate from
// CommitLease until the endpoint is promoted to a stable API.
func (s *Services) GraftLease(ctx context.Context, token, oldRootHash, newRootHash string, tag gw.RepositoryTag) (uint64, error) {
	return s.commitLease(ctx, token, oldRootHash, newRootHash, tag, true)
}

// commitLease is the shared implementation behind CommitLease and GraftLease.
func (s *Services) commitLease(ctx context.Context, token, oldRootHash, newRootHash string, tag gw.RepositoryTag, directGraft bool) (uint64, error) {
	t0 := time.Now()

	action := "commit_lease"
	if directGraft {
		action = "graft_lease"
	}

	outcome := "success"
	defer logAction(ctx, action, &outcome, t0)

	// Look up and validate the lease. leaseMutex is only held for this short
	// SQLite read, not for the (potentially very slow) commit below, so that
	// concurrent lease listing/acquisition is not blocked behind a commit.
	lease, err := func() (*Lease, error) {
		leaseMutex.Lock()
		defer leaseMutex.Unlock()

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

		return lease, nil
	}()
	if err != nil {
		return 0, err
	}

	// Perform the actual commit through the receiver. This can take a long
	// time, so it must run without holding leaseMutex. The per-repository
	// commit lock still serialises commits (and GC) for the same repository.
	//
	// The commit deadline is passed to the receiver, which re-checks it just
	// before publishing: if the lease expired while this (slow) commit was
	// running, the receiver does not modify the repository, so an overlapping
	// lease granted to another publisher in the meantime cannot be overwritten.
	// The configured safety margin is subtracted from the lease expiration so the
	// commit is refused slightly before the lease actually expires.
	commitDeadline := lease.Expiration.Add(-s.Config.CommitLeaseExpiryMargin)
	var finalRev uint64
	if err := s.DB.WithLock(ctx, lease.Repository, func() error {
		var err error
		leasePath := lease.CombinedLeasePath()
		if directGraft {
			finalRev, err = s.Pool.GraftLease(ctx, leasePath, oldRootHash, newRootHash, tag, commitDeadline)
		} else {
			finalRev, err = s.Pool.CommitLease(ctx, leasePath, oldRootHash, newRootHash, tag, commitDeadline)
		}
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

	// Remove the now-committed lease. Again only a short SQLite write under
	// leaseMutex.
	if err := func() error {
		leaseMutex.Lock()
		defer leaseMutex.Unlock()

		tx, err := s.DB.SQL.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("could not begin transaction: %w", err)
		}
		defer tx.Rollback()

		if err := DeleteLeaseByToken(ctx, tx, token); err != nil {
			outcome = err.Error()
			return err
		}

		if err := tx.Commit(); err != nil {
			return fmt.Errorf("could not commit transaction: %w", err)
		}

		return nil
	}(); err != nil {
		return finalRev, err
	}

	return finalRev, nil
}
