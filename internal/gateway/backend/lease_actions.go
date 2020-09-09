package backend

import (
	"context"
	"fmt"
	"time"

	gw "github.com/cvmfs/gateway/internal/gateway"
)

// LeaseReturn is the response type of lease queries, handed
// back to the HTTP frontend
type LeaseReturn struct {
	KeyID     string `json:"key_id,omitempty"`
	LeasePath string `json:"path,omitempty"`
	Expires   string `json:"expires,omitempty"`
}

// NewLease for the specified path, using keyID
func (s *Services) NewLease(ctx context.Context, keyID, leasePath string, protocolVersion int) (string, error) {
	t0 := time.Now()

	outcome := "success"
	defer logAction(ctx, "new_lease", &outcome, t0)

	repoName, subPath, err := gw.SplitLeasePath(leasePath)
	if err != nil {
		outcome = err.Error()
		return "", err
	}

	// Check if keyID is allowed to request a lease in the repository
	// at the specified subpath
	if err := s.Access.Check(keyID, subPath, repoName); err != nil {
		outcome = err.Error()
		return "", err
	}

	// Generate a new token for the lease
	token, err := NewLeaseToken(leasePath, s.Config.MaxLeaseTime)
	if err != nil {
		outcome = err.Error()
		return "", err
	}

	if err := s.Leases.NewLease(ctx, keyID, leasePath, protocolVersion, *token); err != nil {
		outcome = err.Error()
		return "", err
	}

	// the StatsMgr does not handle the case in which a lease expires.
	// However, if a lease expires, we should not upload it's statistics.
	// If the LeaseMgr successfully create a new lease,
	// then, the lease path must be free.
	// We remove it, no matter what.
	// We don't check the error because it return an error if the lease does not exist, the standard case.
	s.StatsMgr.PopLease(leasePath)

	if err := s.StatsMgr.CreateLease(leasePath); err != nil {
		outcome = err.Error()
		return "", err
	}

	outcome = fmt.Sprintf("success: %v", token.TokenStr)
	return token.TokenStr, err
}

// GetLeases returns all active and valid leases
func (s *Services) GetLeases(ctx context.Context) (map[string]LeaseReturn, error) {
	t0 := time.Now()

	outcome := "success"
	defer logAction(ctx, "get_leases", &outcome, t0)

	leases, err := s.Leases.GetLeases(ctx)
	if err != nil {
		outcome = err.Error()
		return nil, err
	}
	ret := make(map[string]LeaseReturn)
	for k, v := range leases {
		if err := CheckToken(v.Token.TokenStr, v.Token.Secret); err == nil {
			ret[k] = LeaseReturn{KeyID: v.KeyID, Expires: v.Token.Expiration.String()}
		}
	}
	return ret, nil
}

// GetLease returns the lease associated with a token
func (s *Services) GetLease(ctx context.Context, tokenStr string) (*LeaseReturn, error) {
	t0 := time.Now()

	outcome := "success"
	defer logAction(ctx, "get_lease", &outcome, t0)

	leasePath, lease, err := s.Leases.GetLease(ctx, tokenStr)
	if err != nil {
		outcome = err.Error()
		return nil, err
	}

	if err := CheckToken(tokenStr, lease.Token.Secret); err != nil {
		outcome = err.Error()
		return nil, err
	}

	ret := &LeaseReturn{
		KeyID:     lease.KeyID,
		LeasePath: leasePath,
		Expires:   lease.Token.Expiration.String(),
	}
	return ret, nil
}

// CancelLeases cancels all the active leases below a repository path
func (s *Services) CancelLeases(ctx context.Context, repoPath string) error {
	t0 := time.Now()

	outcome := "success"
	defer logAction(ctx, "cancel_lease", &outcome, t0)

	if err := s.Leases.CancelLeases(ctx, repoPath); err != nil {
		outcome = err.Error()
		return err
	}

	return nil
}

// CancelLease associated with the token (transaction rollback)
func (s *Services) CancelLease(ctx context.Context, tokenStr string) error {
	t0 := time.Now()

	outcome := "success"
	defer logAction(ctx, "cancel_lease", &outcome, t0)

	leasePath, lease, err := s.Leases.GetLease(ctx, tokenStr)
	if err != nil {
		outcome = err.Error()
		return err
	}

	if err := CheckToken(tokenStr, lease.Token.Secret); err != nil {
		// Allow an expired token to be used to cancel a lease
		if _, ok := err.(ExpiredTokenError); !ok {
			outcome = err.Error()
			return err
		}
	}

	if err := s.Leases.CancelLease(ctx, tokenStr); err != nil {
		outcome = err.Error()
		return err
	}

	if _, err := s.StatsMgr.PopLease(leasePath); err != nil {
		outcome = err.Error()
		return err
	}

	return nil
}

// CommitLease associated with the token (transaction commit)
func (s *Services) CommitLease(ctx context.Context, tokenStr, oldRootHash, newRootHash string, tag gw.RepositoryTag) (uint64, error) {
	t0 := time.Now()

	outcome := "success"
	defer logAction(ctx, "commit_lease", &outcome, t0)

	leasePath, lease, err := s.Leases.GetLease(ctx, tokenStr)
	if err != nil {
		outcome = err.Error()
		return 0, err
	}

	if err := CheckToken(tokenStr, lease.Token.Secret); err != nil {
		outcome = err.Error()
		return 0, err
	}

	repository, _, err := gw.SplitLeasePath(leasePath)
	if err != nil {
		outcome = err.Error()
		return 0, err
	}
	var finalRev uint64
	if err := s.Leases.WithLock(ctx, repository, func() error {
		var err error
		finalRev, err = s.Pool.CommitLease(ctx, leasePath, oldRootHash, newRootHash, tag)
		return err
	}); err != nil {
		outcome = err.Error()
		return 0, err
	}

	go func() {
		plotsErr := s.StatsMgr.UploadStatsPlots(repository)
		if plotsErr != nil {
			gw.LogC(ctx, "actions", gw.LogError).Msgf(plotsErr.Error())
		}
	}()

	if err := s.Leases.CancelLease(ctx, tokenStr); err != nil {
		outcome = err.Error()
		return finalRev, err
	}

	return finalRev, nil
}
