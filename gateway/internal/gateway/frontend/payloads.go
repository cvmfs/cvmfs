package frontend

import (
	"encoding/json"
	"io"
	"net/http"
	"strconv"

	gw "github.com/cvmfs/gateway/internal/gateway"
	be "github.com/cvmfs/gateway/internal/gateway/backend"
	"github.com/julienschmidt/httprouter"
)

// MakePayloadsHandler creates an HTTP handler for the API root
func MakePayloadsHandler(services be.ActionController) httprouter.Handle {
	return func(w http.ResponseWriter, h *http.Request, ps httprouter.Params) {
		token := ps.ByName("token")

		ctx := h.Context()

		msgSize, err := strconv.Atoi(h.Header.Get("message-size"))
		if err != nil {
			httpWrapError(ctx, err, "missing message-size header", w, http.StatusBadRequest)
			return
		}

		var req struct {
			TokenStr   string `json:"session_token"`
			Digest     string `json:"payload_digest"`
			HeaderSize string `json:"header_size"` // cvmfs_swissknife sends this field as a string
			Version    string `json:"api_version"` // cvmfs_swissknife sends this field as a string
		}

		msgRdr := io.LimitReader(h.Body, int64(msgSize))
		if err := json.NewDecoder(msgRdr).Decode(&req); err != nil {
			httpWrapError(ctx, err, "invalid request body", w, http.StatusBadRequest)
			return
		}
		headerSize, err := strconv.Atoi(req.HeaderSize)
		if err != nil {
			httpWrapError(ctx, err, "invalid header_size", w, http.StatusBadRequest)
			return
		}

		if token == "" {
			// For legacy style requests, the token is provided in the request message
			token = req.TokenStr
		}

		msg := make(map[string]interface{})
		if err := services.SubmitPayload(ctx, token, h.Body, req.Digest, headerSize); err != nil {
			msg["status"] = "error"
			msg["reason"] = err.Error()
		} else {
			msg["status"] = "ok"
		}

		gw.LogC(ctx, "http", gw.LogInfo).Msg("request_processed")

		replyJSON(ctx, w, msg)
	}
}
