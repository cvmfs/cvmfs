package frontend

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strings"

	gw "github.com/cvmfs/gateway/internal/gateway"
	be "github.com/cvmfs/gateway/internal/gateway/backend"
	"github.com/julienschmidt/httprouter"
)

type reply map[string]interface{}

// MakeNotificationsHandler creates an HTTP handler for the notifications API
func MakeNotificationsHandler(services be.ActionController) httprouter.Handle {
	return func(w http.ResponseWriter, h *http.Request, ps httprouter.Params) {
		if h.Method == "POST" && strings.HasSuffix(h.URL.Path, "publish") {
			handlePublish(services, w, h, ps)
		} else {
			handleSubscribe(services, w, h, ps)
		}

	}
}

func handlePublish(
	services be.ActionController, w http.ResponseWriter, h *http.Request, ps httprouter.Params) {
	ctx := h.Context()

	var req struct {
		Version    int             `json:"version"`
		Timestamp  json.RawMessage `json:"timestamp"`
		Type       string          `json:"type"`
		Repository string          `json:"repository"`
		Manifest   string          `json:"manifest"`
	}

	var body bytes.Buffer
	if _, err := io.Copy(&body, h.Body); err != nil {
		httpWrapError(ctx, err, "could not read request body", w, http.StatusInternalServerError)
		return
	}

	if err := json.Unmarshal(body.Bytes(), &req); err != nil {
		httpWrapError(ctx, err, "invalid request body", w, http.StatusBadRequest)
		return
	}

	rep := map[string]interface{}{"status": "ok"}

	if err := services.PublishManifest(ctx, req.Repository, body.Bytes()); err != nil {
		rep["status"] = "error"
		rep["reason"] = err.Error()
	}

	gw.LogC(ctx, "http", gw.LogInfo).Msg("request_processed")

	replyJSON(ctx, w, rep)
}

func handleSubscribe(
	services be.ActionController, w http.ResponseWriter, h *http.Request, ps httprouter.Params) {
	ctx := h.Context()

	rep := map[string]interface{}{"status": "ok"}

	gw.LogC(ctx, "http", gw.LogInfo).Msg("request_processed")

	replyJSON(ctx, w, rep)
}
