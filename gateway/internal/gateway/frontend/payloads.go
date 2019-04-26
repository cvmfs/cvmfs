package frontend

import (
	"encoding/json"
	"io"
	"net/http"
	"strconv"

	gw "github.com/cvmfs/gateway/internal/gateway"
	be "github.com/cvmfs/gateway/internal/gateway/backend"
	"github.com/google/uuid"
	"github.com/gorilla/mux"
)

// MakePayloadsHandler creates an HTTP handler for the API root
func MakePayloadsHandler(services *be.Services) http.HandlerFunc {
	return func(w http.ResponseWriter, h *http.Request) {
		token, hasToken := mux.Vars(h)["token"]

		reqID, _ := h.Context().Value(idKey).(uuid.UUID)

		msgSize, err := strconv.Atoi(h.Header.Get("message-size"))
		if err != nil {
			httpWrapError(&reqID, err, "missing message-size header", w, http.StatusBadRequest)
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
			httpWrapError(&reqID, err, "invalid request body", w, http.StatusBadRequest)
			return
		}
		headerSize, err := strconv.Atoi(req.HeaderSize)
		if err != nil {
			httpWrapError(&reqID, err, "invalid header_size", w, http.StatusBadRequest)
			return
		}

		if !hasToken {
			// For legacy style requests, the token is provided in the request message
			token = req.TokenStr
		}

		msg := make(map[string]interface{})
		if err := be.SubmitPayload(services, token, h.Body, req.Digest, headerSize); err != nil {
			msg["status"] = "error"
			msg["reason"] = err.Error()
		} else {
			msg["status"] = "ok"
		}

		frontendLog(ctx, gw.InfoLevel, "request_processed")

		replyJSON(&reqID, w, msg)
	}
}
