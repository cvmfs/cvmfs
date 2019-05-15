package frontend

import (
	"net/http"

	gw "github.com/cvmfs/gateway/internal/gateway"
	be "github.com/cvmfs/gateway/internal/gateway/backend"
	"github.com/julienschmidt/httprouter"
)

// MakeReposHandler creates an HTTP handler for the API root
func MakeReposHandler(services be.ActionController) httprouter.Handle {
	return func(w http.ResponseWriter, h *http.Request, ps httprouter.Params) {
		ctx := h.Context()
		msg := make(map[string]interface{})

		if repoName := ps.ByName("name"); repoName != "" {
			rc := services.GetRepo(repoName)
			if rc == nil {
				msg["status"] = "error"
				msg["reason"] = "invalid_repo"
			} else {
				msg["status"] = "ok"
				msg["data"] = rc
			}
		} else {
			msg["status"] = "ok"
			msg["data"] = services.GetRepos()
		}

		gw.LogC(ctx, "http", gw.LogInfo).Msg("request processed")

		replyJSON(ctx, w, msg)
	}
}
