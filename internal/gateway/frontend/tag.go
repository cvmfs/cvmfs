package frontend

import (
	"context"
	"net/http"
	"time"

	gw "github.com/cvmfs/gateway/internal/gateway"
	"github.com/google/uuid"
	"github.com/gorilla/mux"
)

// MakeTaggingMiddleware returns a gorilla/mux middleware that tags requests
// with an UUID and the time the request was received
func MakeTaggingMiddleware() mux.MiddlewareFunc {
	return mux.MiddlewareFunc(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			reqID := uuid.New()
			gw.Log.Info().
				Str("component", "http").
				Str("req_id", reqID.String()).
				Str("method", req.Method).
				Str("url", req.URL.String()).
				Msg("request received")
			ctx := context.WithValue(
				context.WithValue(req.Context(), idKey, reqID),
				t0Key, time.Now())
			next.ServeHTTP(w, req.WithContext(ctx))
		})
	})
}
