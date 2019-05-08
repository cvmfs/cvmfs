package frontend

import (
	"context"
	"net/http"
	"time"

	gw "github.com/cvmfs/gateway/internal/gateway"
	"github.com/google/uuid"
	"github.com/julienschmidt/httprouter"
)

// WithTag returns a middleware that tags requests with an UUID and the time the
// request was received
func WithTag(next httprouter.Handle) httprouter.Handle {
	return func(w http.ResponseWriter, req *http.Request, ps httprouter.Params) {
		reqID := uuid.New()
		t0 := time.Now()
		ctx := context.WithValue(
			context.WithValue(req.Context(), gw.IDKey, reqID),
			gw.T0Key, time.Now())
		gw.LogC(ctx, "http", gw.LogInfo).
			Str("method", req.Method).
			Str("url", req.URL.String()).
			Str("t0", t0.Format(time.RFC3339)).
			Msg("request received")
		next(w, req.WithContext(ctx), ps)
	}
}
