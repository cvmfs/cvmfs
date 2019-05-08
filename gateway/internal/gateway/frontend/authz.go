package frontend

import (
	"bytes"
	"encoding/base64"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"

	"github.com/gorilla/mux"

	gw "github.com/cvmfs/gateway/internal/gateway"
	be "github.com/cvmfs/gateway/internal/gateway/backend"
)

type message map[string]interface{}

// MakeAuthzMiddleware returns an HMAC authorization middleware for use with the gorilla/mux server
func MakeAuthzMiddleware(ac be.ActionController) mux.MiddlewareFunc {
	return mux.MiddlewareFunc(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			// GET requests do not need authorization
			if req.Method == "GET" {
				next.ServeHTTP(w, req)
				return
			}

			ctx := req.Context()
			tokens := strings.Split(req.Header.Get("Authorization"), " ")
			if len(tokens) != 2 {
				gw.LogC(ctx, "http", gw.LogError).
					Msg("missing tokens in authorization header")
				replyJSON(ctx, w, message{"status": "error", "reason": "invalid_hmac"})
				return
			}

			keyID := tokens[0]
			HMAC, err := base64.StdEncoding.DecodeString(tokens[1])
			if err != nil {
				gw.LogC(ctx, "http", gw.LogError).
					Err(err).Msg("could not base64 decode HMAC")
				replyJSON(ctx, w, message{"status": "error", "reason": "invalid_hmac"})
				return
			}

			secret := ac.GetSecret(keyID)
			if len(secret) == 0 {
				gw.LogC(ctx, "http", gw.LogError).
					Msg("invalid key ID specified")
				replyJSON(ctx, w, message{"status": "error", "reason": "invalid_hmac"})
				return
			}

			// Different parts of the request are used to compute then HMAC, depending
			// in HTTP method and route

			var HMACInput []byte
			if strings.HasPrefix(req.URL.Path, APIRoot+"/leases") {
				token, _ := mux.Vars(req)["token"]
				if token != "" {
					// For commit/drop lease requests use the token to compute HMAC
					HMACInput = []byte(token)
				} else {
					// For new lease request used the request body to compute HMAC
					HMACInput, err = ioutil.ReadAll(req.Body)
					if err != nil {
						httpWrapError(ctx, err, "could not read request body", w, http.StatusInternalServerError)
						return
					}
					// Body needs to be read again in the next handler, reset it
					// using a copy of the original body
					bodyCopy := ioutil.NopCloser(bytes.NewReader(HMACInput))
					req.Body.Close()
					req.Body = bodyCopy
				}
			} else if strings.HasPrefix(req.URL.Path, APIRoot+"/payloads") {
				token, _ := mux.Vars(req)["token"]
				if token != "" {
					// For the new style of payload submission requests, use the token to compute HMAC
					HMACInput = []byte(token)
				} else {
					// For legacy payload submission requests, the JSON msg at the beginning of the body
					// is used to compute the HMAC
					sz := req.Header.Get("message-size")
					msgSize, err := strconv.Atoi(sz)
					if err != nil {
						httpWrapError(ctx, err, "missing message-size header", w, http.StatusBadRequest)
						return
					}
					msg := make([]byte, msgSize)
					if _, err := io.ReadFull(req.Body, msg); err != nil {
						httpWrapError(ctx, err, "invalid request body", w, http.StatusBadRequest)
						return
					}

					HMACInput = msg

					// replace the request body with a new ReadCLoser which includes the already-read
					// head part
					req.Body = newRecombineReadCloser(msg, req.Body)
				}
			}

			if !CheckHMAC(HMACInput, HMAC, secret) {
				gw.LogC(ctx, "http", gw.LogError).
					Msg("invalid HMAC")
				replyJSON(ctx, w, message{"status": "error", "reason": "invalid_hmac"})
				return
			}
			next.ServeHTTP(w, req)
		})
	})
}

// The recombineReadCloser is used during payload submission requests to recombine the request message,
// already read inside the authorization middleware with the remaining request body and ensure that the
// body (io.ReadCloser) is eventually closed and does not leak
type recombineReadCloser struct {
	combined io.Reader
	original io.ReadCloser
}

func newRecombineReadCloser(head []byte, tail io.ReadCloser) *recombineReadCloser {
	return &recombineReadCloser{io.MultiReader(bytes.NewReader(head), tail), tail}
}

func (r recombineReadCloser) Read(p []byte) (int, error) {
	return r.combined.Read(p)
}

func (r recombineReadCloser) Close() error {
	return r.original.Close()
}
