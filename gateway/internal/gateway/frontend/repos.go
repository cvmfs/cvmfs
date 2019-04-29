package frontend

import (
	"net/http"

	gw "github.com/cvmfs/gateway/internal/gateway"
	be "github.com/cvmfs/gateway/internal/gateway/backend"
	"github.com/gorilla/mux"
)

// MakeReposHandler creates an HTTP handler for the API root
func MakeReposHandler(services be.ActionController) http.HandlerFunc {
	return func(w http.ResponseWriter, h *http.Request) {
		ctx := h.Context()
		vs := mux.Vars(h)

		msg := make(map[string]interface{})

		if repoName, present := vs["name"]; present {
			r := services.GetRepo(repoName)
			if len(r) == 0 {
				msg["status"] = "error"
				msg["reason"] = "invalid_repo"
			} else {
				msg["status"] = "ok"
				msg["data"] = r
			}
		} else {
			msg["status"] = "ok"
			msg["data"] = services.GetRepos()
		}

		gw.LogC(ctx, "http", gw.LogInfo).Msg("request processed")

		replyJSON(ctx, w, msg)
	}
}
