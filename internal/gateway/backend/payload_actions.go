package backend

import (
	"context"
	"io"
	"time"

	gw "github.com/cvmfs/gateway/internal/gateway"
)

// SubmitPayload to be unpacked into the repository
func SubmitPayload(ctx context.Context, s *Services, token string, payload io.Reader, digest string, headerSize int) error {
	t0 := time.Now()

	outcome := "success"
	defer func() {
		gw.LogC(ctx, "actions", gw.LogInfo).
			Str("action", "submit_payload").
			Str("outcome", outcome).
			Float64("action_dt", time.Now().Sub(t0).Seconds()).
			Msg("action complete")
	}()

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
