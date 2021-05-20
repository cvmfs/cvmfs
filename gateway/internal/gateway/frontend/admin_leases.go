package frontend

import (
	"net/http"
	"strings"

	gw "github.com/cvmfs/gateway/internal/gateway"
	be "github.com/cvmfs/gateway/internal/gateway/backend"
	"github.com/julienschmidt/httprouter"
)

// MakeAdminLeasesHandler creates an HTTP handler for the API root
func MakeAdminLeasesHandler(services be.ActionController) httprouter.Handle {
	return func(w http.ResponseWriter, h *http.Request, ps httprouter.Params) {
		ctx := h.Context()

		msg := map[string]interface{}{"status": "ok"}

		repoPath := strings.TrimPrefix(ps.ByName("path"), "/")
		if repoPath == "" {
			errMsg := "missing path argument"
			gw.LogC(ctx, "http", gw.LogError).Msg(errMsg)
			http.Error(w, errMsg, http.StatusBadRequest)
			return
		}

		if err := services.CancelLeases(ctx, repoPath); err != nil {
			msg["status"] = "error"
			msg["reason"] = err.Error()
		}

		replyJSON(ctx, w, msg)
	}
}
