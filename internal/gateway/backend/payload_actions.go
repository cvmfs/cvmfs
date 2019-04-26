package backend

import (
	"context"
	"io"
)

// SubmitPayload to be unpacked into the repository
func SubmitPayload(ctx context.Context, s *Services, token string, payload io.Reader, digest string, headerSize int) error {
	leasePath, lease, err := s.Leases.GetLease(ctx, token)
	if err != nil {
		return err
	}

	if err := CheckToken(token, lease.Token.Secret); err != nil {
		return err
	}

	if err := s.Pool.SubmitPayload(ctx, leasePath, payload, digest, headerSize); err != nil {
		return err
	}

	return nil
}
