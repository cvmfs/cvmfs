package backend

import (
	"fmt"

	gw "github.com/cvmfs/gateway/internal/gateway"
	"github.com/pkg/errors"
)

// LeaseReturn is the response type of lease queries, handed
// back to the HTTP frontend
type LeaseReturn struct {
	KeyID     string `json:"key_id,omitempty"`
	LeasePath string `json:"path,omitempty"`
	Expires   string `json:"expires,omitempty"`
}

// NewLease for the specified path, using keyID
func NewLease(s *Services, keyID, leasePath string) (string, error) {
	repoName, subPath, err := gw.SplitLeasePath(leasePath)
	if err != nil {
		return "", errors.Wrap(err, "could not parse lease path")
	}

	// Check if keyID is allowed to request a lease in the repository
	// at the specified subpath
	if err := s.Access.Check(keyID, subPath, repoName); err != nil {
		return "", err
	}

	// Generate a new token for the lease
	token, err := NewLeaseToken(leasePath, s.Config.MaxLeaseTime)
	if err != nil {
		return "", errors.Wrap(err, "could not generate session token")
	}

	if err := s.Leases.NewLease(keyID, leasePath, *token); err != nil {
		return "", err
	}

	return token.TokenStr, nil
}

// GetLeases returns all active and valid leases
func GetLeases(s *Services) (map[string]LeaseReturn, error) {
	leases, err := s.Leases.GetLeases()
	if err != nil {
		return nil, err
	}
	ret := make(map[string]LeaseReturn)
	for k, v := range leases {
		if _, err := CheckToken(v.Token.TokenStr, v.Token.Secret); err == nil {
			ret[k] = LeaseReturn{KeyID: v.KeyID, Expires: v.Token.Expiration.String()}
		}
	}
	return ret, nil
}

// GetLease returns the lease associated with a token
func GetLease(s *Services, tokenStr string) (*LeaseReturn, error) {
	leasePath, lease, err := s.Leases.GetLease(tokenStr)
	if err != nil {
		return nil, err
	}

	if _, err := CheckToken(tokenStr, lease.Token.Secret); err != nil {
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
func CancelLease(s *Services, tokenStr string) error {
	_, lease, err := s.Leases.GetLease(tokenStr)
	if err != nil {
		return err
	}

	if _, err := CheckToken(tokenStr, lease.Token.Secret); err != nil {
		// Allow an expired token to be used to cancel a lease
		if _, ok := err.(ExpiredTokenError); !ok {
			return err
		}
	}

	if err := s.Leases.CancelLease(tokenStr); err != nil {
		return err
	}

	return nil
}

// CommitLease associated with the token (transaction commit)
func CommitLease(s *Services, leasePath, oldRootHash, newRootHash string, tag gw.RepositoryTag) error {
	return fmt.Errorf("not implemented")
}
