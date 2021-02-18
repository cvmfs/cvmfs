package frontend

import (
	"encoding/json"
	"net/http"

	gw "github.com/cvmfs/gateway/internal/gateway"
	be "github.com/cvmfs/gateway/internal/gateway/backend"
	"github.com/julienschmidt/httprouter"
)

// MakeGCHandler creates an HTTP handler for the "/gc" endpoint
func MakeGCHandler(services be.ActionController) httprouter.Handle {
	return func(w http.ResponseWriter, h *http.Request, ps httprouter.Params) {
		ctx := h.Context()

		var options be.GCOptions
		if err := json.NewDecoder(h.Body).Decode(&options); err != nil {
			httpWrapError(ctx, err, "invalid request body", w, http.StatusBadRequest)
			return
		}

		msg := map[string]interface{}{"status": "ok"}
		if output, err := services.RunGC(ctx, options); err != nil {
			msg["status"] = "error"
			msg["reason"] = err.Error()
		} else {
			msg["output"] = output
		}

		gw.LogC(ctx, "http", gw.LogInfo).Msg("request processed")

		replyJSON(ctx, w, msg)
	}
}
