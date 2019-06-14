package frontend

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"

	"github.com/julienschmidt/httprouter"
	"github.com/pkg/errors"

	gw "github.com/cvmfs/gateway/internal/gateway"
	be "github.com/cvmfs/gateway/internal/gateway/backend"
)

type message map[string]interface{}

// WithAdminAuthz returns an HMAC authorization middleware used for administrative
// operations (disable/enable repositories and keys, cancel leases, trigger GC, etc.)
func WithAdminAuthz(ac be.ActionController, next httprouter.Handle) httprouter.Handle {
	return func(w http.ResponseWriter, req *http.Request, ps httprouter.Params) {
		ctx := req.Context()
		keyID, HMAC, err := parseHeader(&req.Header)
		if err != nil {
			gw.LogC(ctx, "http", gw.LogError).
				Err(err).
				Msg("authorization failure")
			replyJSON(ctx, w, message{"status": "error", "reason": "invalid_authorization_header"})
			return
		}

		keyCfg := ac.GetKey(ctx, keyID)
		if keyCfg == nil {
			gw.LogC(ctx, "http", gw.LogError).
				Msg("invalid key ID specified")
			replyJSON(ctx, w, message{"status": "error", "reason": "invalid_key"})
			return
		}

		if !keyCfg.Admin {
			gw.LogC(ctx, "http", gw.LogError).
				Msg("key does not have admin rights")
			replyJSON(ctx, w, message{"status": "error", "reason": "no_admin_key"})
			return
		}

		var HMACInput []byte
		switch req.Method {
		case "DELETE":
			// For DELETE requests, use the path component of the URL to compute the HMAC
			HMACInput = []byte(req.URL.Path)
		case "POST":
			// For POST requests, the request body is used to compute HMAC
			HMACInput, err = readBody(req, req.ContentLength)
			if err != nil {
				httpWrapError(ctx, err, "could not read request body", w, http.StatusInternalServerError)
				return
			}
		default:
			msg := fmt.Sprintf("authorization middleware not implemented for HTTP method: %v", req.Method)
			gw.LogC(ctx, "http", gw.LogError).
				Msgf(msg)
			http.Error(w, msg, http.StatusMethodNotAllowed)
		}

		if !CheckHMAC(HMACInput, HMAC, keyCfg.Secret) {
			gw.LogC(ctx, "http", gw.LogError).
				Msg("invalid HMAC")
			replyJSON(ctx, w, message{"status": "error", "reason": "invalid_hmac"})
			return
		}

		next(w, req, ps)
	}
}

// WithAuthz returns an HMAC authorization middleware
func WithAuthz(ac be.ActionController, next httprouter.Handle) httprouter.Handle {
	return func(w http.ResponseWriter, req *http.Request, ps httprouter.Params) {
		ctx := req.Context()
		keyID, HMAC, err := parseHeader(&req.Header)
		if err != nil {
			gw.LogC(ctx, "http", gw.LogError).
				Err(err).
				Msg("authorization failure")
			replyJSON(ctx, w, message{"status": "error", "reason": "invalid_hmac"})
			return
		}

		keyCfg := ac.GetKey(ctx, keyID)
		if keyCfg == nil {
			gw.LogC(ctx, "http", gw.LogError).
				Msg("invalid key ID specified")
			replyJSON(ctx, w, message{"status": "error", "reason": "invalid_hmac"})
			return
		}

		// Different parts of the request are used to compute then HMAC, depending
		// in HTTP method and route

		var HMACInput []byte
		if strings.HasPrefix(req.URL.Path, APIRoot+"/leases") {
			token := ps.ByName("token")
			if token != "" {
				// For commit/drop lease requests use the token to compute HMAC
				HMACInput = []byte(token)
			} else {
				// For new lease request used the request body to compute HMAC
				HMACInput, err = readBody(req, req.ContentLength)
				if err != nil {
					httpWrapError(ctx, err, "could not read request body", w, http.StatusInternalServerError)
					return
				}
			}
		} else if strings.HasPrefix(req.URL.Path, APIRoot+"/payloads") {
			token := ps.ByName("token")
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
				HMACInput, err = readBody(req, int64(msgSize))
				if err != nil {
					httpWrapError(ctx, err, "could not read request body", w, http.StatusInternalServerError)
					return
				}
			}
		}

		if !CheckHMAC(HMACInput, HMAC, keyCfg.Secret) {
			gw.LogC(ctx, "http", gw.LogError).
				Msg("invalid HMAC")
			replyJSON(ctx, w, message{"status": "error", "reason": "invalid_hmac"})
			return
		}
		next(w, req, ps)
	}
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

func parseHeader(h *http.Header) (string, []byte, error) {
	tokens := strings.Split(h.Get("Authorization"), " ")
	if len(tokens) != 2 {
		return "", nil, fmt.Errorf("missing tokens in authoriation header")
	}

	keyID := tokens[0]

	HMAC, err := base64.StdEncoding.DecodeString(tokens[1])
	if err != nil {
		return "", nil, errors.Wrap(err, "could not base64 decode HMAC")
	}

	return keyID, HMAC, nil
}

// Read the request body and place and resets the consumed Reader, allowing the
// body to be re-read in the next HTTP handler. Only the first "readSize" bytes
// will be read from the body and returned.
func readBody(req *http.Request, readSize int64) ([]byte, error) {
	partial := readSize < req.ContentLength
	body := make([]byte, readSize)
	if _, err := io.ReadFull(req.Body, body); err != nil {
		return nil, err
	}

	if partial {
		// replace the request body with a new ReadCLoser which includes the already-read
		// head part
		req.Body = newRecombineReadCloser(body, req.Body)
	} else {
		bodyCopy := ioutil.NopCloser(bytes.NewReader(body))
		req.Body.Close()
		req.Body = bodyCopy
	}

	return body, nil
}
