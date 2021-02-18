package backend

import (
	"context"
	"io"
	"time"
)

// SubmitPayload to be unpacked into the repository
func (s *Services) SubmitPayload(ctx context.Context, token string, payload io.Reader, digest string, headerSize int) error {
	t0 := time.Now()

	outcome := "success"
	defer logAction(ctx, "submit_payload", &outcome, t0)

	leasePath, lease, err := s.Leases.GetLease(ctx, token)
	if err != nil {
		outcome = err.Error()
		return err
	}

	if err := CheckToken(token, lease.Token.Secret); err != nil {
		outcome = err.Error()
		return err
	}

	if err := s.Pool.SubmitPayload(ctx, leasePath, payload, digest, headerSize); err != nil {
		outcome = err.Error()
		return err
	}

	return nil
}
