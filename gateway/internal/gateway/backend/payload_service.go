package backend

import (
	"context"
	"fmt"
	"io"
	"time"
)

// SubmitPayload to be unpacked into the repository
func (s *Services) SubmitPayload(ctx context.Context, token string, payload io.Reader, digest string, headerSize int) error {
	t0 := time.Now()

	outcome := "success"
	defer logAction(ctx, "submit_payload", &outcome, t0)

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

	if lease == nil || lease.Expiration.Before(time.Now()) {
		err := InvalidLeaseError{}
		outcome = err.Error()
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("could not commit transaction: %w", err)
	}

	if lease == nil {
		return fmt.Errorf("lease not found: %w", err)
	}

	if err := s.Pool.SubmitPayload(ctx, lease.CombinedLeasePath(), payload, digest, headerSize); err != nil {
		outcome = err.Error()
		return err
	}
	return nil
}
