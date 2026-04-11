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

// MakeCatalogsHandler creates an HTTP handler for the catalog-only submission
// endpoint.  This is used by publishers that upload data chunks directly to S3
// and only need the gateway to process catalogs.
func MakeCatalogsHandler(services be.ActionController) httprouter.Handle {
	return func(w http.ResponseWriter, h *http.Request, ps httprouter.Params) {
		token := ps.ByName("token")
		if token == "" {
			http.Error(w, "missing token", http.StatusBadRequest)
			return
		}

		ctx := h.Context()

		msgSize, err := strconv.Atoi(h.Header.Get("message-size"))
		if err != nil {
			httpWrapError(ctx, err, "missing message-size header", w, http.StatusBadRequest)
			return
		}

		var req struct {
			Digest     string `json:"payload_digest"`
			HeaderSize string `json:"header_size"`
			Version    string `json:"api_version"`
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

		msg := make(map[string]interface{})
		if err := services.SubmitPayload(ctx, token, h.Body, req.Digest, headerSize); err != nil {
			msg["status"] = "error"
			msg["reason"] = err.Error()
		} else {
			msg["status"] = "ok"
		}

		gw.LogC(ctx, "http", gw.LogInfo).Msg("catalog_submission_processed")

		replyJSON(ctx, w, msg)
	}
}
