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
func NewLease(ctx context.Context, s *Services, keyID, leasePath string) (string, error) {
	t0 := time.Now()

	outcome := "success"
	defer func() {
		gw.LogC(ctx, "actions", gw.LogInfo).
			Str("action", "new_lease").
			Str("outcome", outcome).
			Float64("action_dt", time.Now().Sub(t0).Seconds()).
			Msg("action complete")
	}()

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

	if err := s.Leases.NewLease(ctx, keyID, leasePath, *token); err != nil {
		outcome = err.Error()
		return "", err
	}

	outcome = fmt.Sprintf("success: %v", token.TokenStr)
	return token.TokenStr, err
}

// GetLeases returns all active and valid leases
func GetLeases(ctx context.Context, s *Services) (map[string]LeaseReturn, error) {
	t0 := time.Now()

	outcome := "success"
	defer func() {
		gw.LogC(ctx, "actions", gw.LogInfo).
			Str("action", "get_leases").
			Str("outcome", outcome).
			Float64("action_dt", time.Now().Sub(t0).Seconds()).
			Msg("action complete")
	}()

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
func GetLease(ctx context.Context, s *Services, tokenStr string) (*LeaseReturn, error) {
	t0 := time.Now()

	outcome := "success"
	defer func() {
		gw.LogC(ctx, "actions", gw.LogInfo).
			Str("action", "get_lease").
			Str("outcome", outcome).
			Float64("action_dt", time.Now().Sub(t0).Seconds()).
			Msg("action complete")
	}()

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

// CancelLease associated with the token (transaction rollback)
func CancelLease(ctx context.Context, s *Services, tokenStr string) error {
	t0 := time.Now()

	outcome := "success"
	defer func() {
		gw.LogC(ctx, "actions", gw.LogInfo).
			Str("action", "cancel_lease").
			Str("outcome", outcome).
			Float64("action_dt", time.Now().Sub(t0).Seconds()).
			Msg("action complete")
	}()

	_, lease, err := s.Leases.GetLease(ctx, tokenStr)
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

	return nil
}

// CommitLease associated with the token (transaction commit)
func CommitLease(ctx context.Context, s *Services, tokenStr, oldRootHash, newRootHash string, tag gw.RepositoryTag) error {
	t0 := time.Now()

	outcome := "success"
	defer func() {
		gw.LogC(ctx, "actions", gw.LogInfo).
			Str("action", "commit_lease").
			Str("outcome", outcome).
			Float64("action_dt", time.Now().Sub(t0).Seconds()).
			Msg("action complete")
	}()

	leasePath, lease, err := s.Leases.GetLease(ctx, tokenStr)
	if err != nil {
		outcome = err.Error()
		return err
	}

	if err := CheckToken(tokenStr, lease.Token.Secret); err != nil {
		outcome = err.Error()
		return err
	}

	repository, _, err := gw.SplitLeasePath(leasePath)
	if err != nil {
		outcome = err.Error()
		return err
	}
	if err := s.Leases.WithLock(ctx, repository, func() error {
		return s.Pool.CommitLease(ctx, leasePath, oldRootHash, newRootHash, tag)
	}); err != nil {
		outcome = err.Error()
		return err
	}

	if err := s.Leases.CancelLease(ctx, tokenStr); err != nil {
		outcome = err.Error()
		return err
	}

	return nil
}
