package frontend

import (
	"encoding/base64"
	"net/http"
	"strings"

	"github.com/google/uuid"
	"github.com/gorilla/mux"

	gw "github.com/cvmfs/gateway/internal/gateway"
	be "github.com/cvmfs/gateway/internal/gateway/backend"
)

// MakeAuthzMiddleware returns an HMAC authorization middleware for use with the gorilla/mux server
func MakeAuthzMiddleware(ac *be.AccessConfig) mux.MiddlewareFunc {
	return mux.MiddlewareFunc(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			reqID := req.Context().Value(idKey).(uuid.UUID)
			tokens := strings.Split(req.Header.Get("Authorization"), " ")
			if len(tokens) != 2 {
				gw.Log.Error().
					Str("component", "http").
					Str("req_id", reqID.String()).
					Msg("missing tokens in authorization header")
				replyJSON(&reqID, w, message{"status": "error", "reason": "invalid_hmac"})
				return
			}

			keyID := tokens[0]
			HMAC, err := base64.StdEncoding.DecodeString(tokens[1])
			if err != nil {
				gw.Log.Error().
					Str("component", "http").
					Str("req_id", reqID.String()).
					Err(err).Msg("could not base64 decode HMAC")
				replyJSON(&reqID, w, message{"status": "error", "reason": "invalid_hmac"})
				return
			}

			secret := ac.GetSecret(keyID)
			if len(secret) == 0 {
				gw.Log.Error().
					Str("component", "http").
					Str("req_id", reqID.String()).
					Msg("invalid key ID specified")
				replyJSON(&reqID, w, message{"status": "error", "reason": "invalid_hmac"})
				return
			}

			// Different parts of the request are used to compute then HMAC, depending
			// in HTTP method and route

			if strings.HasPrefix(req.URL.Path, APIRoot+"/repos") {
				// /repos request, use the path component of the URL to compute HMAC
				if !CheckHMAC([]byte(req.URL.Path), HMAC, secret) {
					gw.Log.Error().
						Str("component", "http").
						Str("req_id", reqID.String()).
						Msg("invalid HMAC")
					replyJSON(&reqID, w, message{"status": "error", "reason": "invalid_hmac"})
					return
				}
			}
			next.ServeHTTP(w, req)
		})
	})
}
