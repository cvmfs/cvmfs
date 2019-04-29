package backend

import (
	"context"
	"time"

	gw "github.com/cvmfs/gateway/internal/gateway"
)

func logAction(ctx context.Context, actionName string, outcome *string, t0 time.Time) {
	gw.LogC(ctx, "actions", gw.LogInfo).
		Str("action", actionName).
		Str("outcome", *outcome).
		Float64("action_dt", time.Now().Sub(t0).Seconds()).
		Msg("action complete")
}
