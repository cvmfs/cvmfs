package backend

import (
	"context"
	"fmt"
	"time"

	gw "github.com/cvmfs/gateway/internal/gateway"
)

// leaseNamedLocks provides per-repository mutual exclusion for lease
// operations (NewLease, CancelLease, CommitLease, CancelLeases).
// Using per-repo locks instead of a single global mutex allows publishers
// working on different repositories to proceed concurrently.
var leaseNamedLocks NamedLocks

// LeaseDTO is the lease information returned to the HTTP frontend
type LeaseDTO struct {
	KeyID     string `json:"key_id,omitempty"`
	LeasePath string `json:"path,omitempty"`
	Expires   string `json:"expires,omitempty"`
	Hostname  string `json:"hostname,omitempty"`
}

// repoForToken returns the repository name associated with a lease token by
// performing a read-only DB lookup.  It returns InvalidLeaseError if the
// lease is missing or has already expired.  No lease lock is held during
// this call; callers must re-validate the lease inside the per-repo lock.
func (s *Services) repoForToken(ctx context.Context, token string) (string, error) {
	tx, err := s.DB.SQL.BeginTx(ctx, nil)
	if err != nil {
		return "", fmt.Errorf("could not begin transaction: %w", err)
	}
	defer tx.Rollback()

	lease, err := FindLeaseByToken(ctx, tx, token)
	if err != nil {
		return "", err
	}
	if lease == nil || lease.Expiration.Before(time.Now()) {
		return "", InvalidLeaseError{}
	}
	return lease.Repository, nil
}

// repoForTokenAny is like repoForToken but also returns the repository for
// expired leases.  Used by CancelLease, which must be able to cancel a lease
// regardless of whether it has expired (matching the original behaviour before
// per-repo locking was introduced).
func (s *Services) repoForTokenAny(ctx context.Context, token string) (string, error) {
	tx, err := s.DB.SQL.BeginTx(ctx, nil)
	if err != nil {
		return "", fmt.Errorf("could not begin transaction: %w", err)
	}
	defer tx.Rollback()

	lease, err := FindLeaseByToken(ctx, tx, token)
	if err != nil {
		return "", err
	}
	if lease == nil {
		return "", InvalidLeaseError{}
	}
	return lease.Repository, nil
}

// NewLease for the specified path, using keyID
func (s *Services) NewLease(ctx context.Context, keyID, leasePath, hostname string, protocolVersion int) (string, error) {
	t0 := time.Now()

	outcome := "success"
	defer logAction(ctx, "new_lease", &outcome, t0)

	repo, path, err := gw.SplitLeasePath(leasePath)
	if err != nil {
		outcome = err.Error()
		return "", err
	}

	var token string
	err = leaseNamedLocks.WithLock(repo, func() error {
		tx, err := s.DB.SQL.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("could not begin transaction: %w", err)
		}
		defer tx.Rollback()

		repoConfig, err := s.GetRepo(ctx, repo)
		if err != nil {
			return fmt.Errorf("could not retrieve repository information: %w", err)
		}
		if repoConfig == nil {
			return fmt.Errorf("repository not found: %s", repo)
		}
		if !repoConfig.Enabled {
			return ErrRepoDisabled
		}

		// Check if keyID is allowed to request a lease in the repository
		// at the specified subpath
		if err := s.Access.Check(keyID, path, repo); err != nil {
			return err
		}

		leases, err := FindAllLeasesByRepositoryAndOverlappingPath(ctx, tx, repo, path)
		if err != nil {
			return err
		}

		for _, lease := range leases {
			timeLeft := time.Until(lease.Expiration)
			if timeLeft > 0 {
				return PathBusyError{timeLeft}
			}
		}

		// Delete expired leases
		if err := DeleteAllExpiredLeases(ctx, tx); err != nil {
			return err
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
			return err
		}

		// The StatsMgr does not handle the case in which a lease expires.
		// However, if a lease expires, we should not upload its statistics.
		// If the LeaseMgr successfully creates a new lease,
		// then the lease path must be free.
		// We remove it, no matter what.
		// We don't check the error because it returns an error if the lease
		// does not exist, the standard case.
		s.StatsMgr.PopLease(lease.CombinedLeasePath())

		if err := s.StatsMgr.CreateLease(lease.CombinedLeasePath()); err != nil {
			return err
		}

		if err := tx.Commit(); err != nil {
			return fmt.Errorf("could not commit transaction: %w", err)
		}

		token = lease.Token
		return nil
	})

	if err != nil {
		outcome = err.Error()
		return "", err
	}
	outcome = fmt.Sprintf("success: %v", token)
	return token, nil
}

// GetLeases returns all active and valid leases.
// Read-only: SQL transaction isolation is sufficient; no lease lock required.
func (s *Services) GetLeases(ctx context.Context) (map[string]LeaseDTO, error) {
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

// GetLease returns the lease associated with a token.
// Read-only: SQL transaction isolation is sufficient; no lease lock required.
func (s *Services) GetLease(ctx context.Context, token string) (*LeaseDTO, error) {
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
	t0 := time.Now()

	outcome := "success"
	defer logAction(ctx, "cancel_leases", &outcome, t0)

	repo, path, err := gw.SplitLeasePath(repoPath)
	if err != nil {
		outcome = err.Error()
		return err
	}

	err = leaseNamedLocks.WithLock(repo, func() error {
		tx, err := s.DB.SQL.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("could not begin transaction: %w", err)
		}
		defer tx.Rollback()

		if err := DeleteAllLeasesByRepositoryAndPathPrefix(ctx, tx, repo, path); err != nil {
			return err
		}

		return tx.Commit()
	})

	if err != nil {
		outcome = err.Error()
	}
	return err
}

// CancelLease associated with the token
func (s *Services) CancelLease(ctx context.Context, token string) error {
	t0 := time.Now()

	outcome := "success"
	defer logAction(ctx, "cancel_lease", &outcome, t0)

	// Pre-read: find the repository for per-repo locking.
	// Use repoForTokenAny so that expired leases (that still exist in the DB)
	// can also be cancelled — matching the pre-refactor behaviour.
	repo, err := s.repoForTokenAny(ctx, token)
	if err != nil {
		outcome = err.Error()
		return err
	}

	err = leaseNamedLocks.WithLock(repo, func() error {
		tx, err := s.DB.SQL.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("could not begin transaction: %w", err)
		}
		defer tx.Rollback()

		// Re-find inside the lock to handle the race between pre-read and lock
		// acquisition (e.g. another goroutine cancelled the same lease first).
		// Do not check expiry: CancelLease must delete the row even if the
		// lease has just expired.
		lease, err := FindLeaseByToken(ctx, tx, token)
		if err != nil {
			return err
		}

		if lease == nil {
			return InvalidLeaseError{}
		}

		if err := DeleteLeaseByToken(ctx, tx, token); err != nil {
			return err
		}

		// We don't check the error - if the statistics are missing, the lease
		// should still be cancelable
		s.StatsMgr.PopLease(lease.CombinedLeasePath())

		return tx.Commit()
	})

	if err != nil {
		outcome = err.Error()
	}
	return err
}

// CommitLease associated with the token (transaction commit)
func (s *Services) CommitLease(ctx context.Context, token, oldRootHash, newRootHash string, tag gw.RepositoryTag) (uint64, error) {
	t0 := time.Now()

	outcome := "success"
	defer logAction(ctx, "commit_lease", &outcome, t0)

	// Pre-read: find the repository for per-repo locking.
	// The lease is re-validated inside the lock.
	repo, err := s.repoForToken(ctx, token)
	if err != nil {
		outcome = err.Error()
		return 0, err
	}

	var finalRev uint64
	err = leaseNamedLocks.WithLock(repo, func() error {
		tx, err := s.DB.SQL.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("could not begin transaction: %w", err)
		}
		defer tx.Rollback()

		// Re-find inside the lock: the lease might have been cancelled or
		// expired between the pre-read and lock acquisition.
		lease, err := FindLeaseByToken(ctx, tx, token)
		if err != nil {
			return err
		}

		if lease == nil || lease.Expiration.Before(time.Now()) {
			return InvalidLeaseError{}
		}

		leasePath := lease.CombinedLeasePath()

		// DB.WithLock serialises commits and GC runs for the same repository.
		if err := s.DB.WithLock(ctx, lease.Repository, func() error {
			var err error
			finalRev, err = s.Pool.CommitLease(ctx, leasePath, oldRootHash, newRootHash, tag)
			return err
		}); err != nil {
			return err
		}

		go func() {
			plotsErr := s.StatsMgr.UploadStatsPlots(lease.Repository)
			if plotsErr != nil {
				gw.LogC(ctx, "actions", gw.LogError).Msgf(plotsErr.Error())
			}
		}()

		if err := DeleteLeaseByToken(ctx, tx, token); err != nil {
			return err
		}

		return tx.Commit()
	})

	if err != nil {
		outcome = err.Error()
		return finalRev, err
	}
	return finalRev, nil
}
