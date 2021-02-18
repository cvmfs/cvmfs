package frontend

import (
	"encoding/json"
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
			rc := services.GetRepo(ctx, repoName)
			if rc == nil {
				msg["status"] = "error"
				msg["reason"] = "invalid_repo"
			} else {
				msg["status"] = "ok"
				msg["data"] = rc
			}
		} else {
			msg["status"] = "ok"
			msg["data"] = services.GetRepos(ctx)
		}

		gw.LogC(ctx, "http", gw.LogInfo).Msg("request processed")

		replyJSON(ctx, w, msg)
	}
}

// MakeAdminReposHandler creates an HTTP handler for the API root
func MakeAdminReposHandler(services be.ActionController) httprouter.Handle {
	return func(w http.ResponseWriter, h *http.Request, ps httprouter.Params) {
		ctx := h.Context()

		var reqMsg struct {
			Enable bool `json:"enable"`
			Wait   bool `json:"wait"`
		}

		if err := json.NewDecoder(h.Body).Decode(&reqMsg); err != nil {
			httpWrapError(ctx, err, "invalid request body", w, http.StatusBadRequest)
			return
		}

		repoName := ps.ByName("name")

		msg := make(map[string]interface{})
		if err := services.SetRepoEnabled(ctx, repoName, reqMsg.Enable); err != nil {
			if _, ok := err.(be.RepoBusyError); ok {
				msg["status"] = "repo_busy"
			} else {
				msg["status"] = "error"
				msg["reason"] = err.Error()
			}
		}

		msg["status"] = "ok"

		gw.LogC(ctx, "http", gw.LogInfo).Msg("request processed")

		replyJSON(ctx, w, msg)
	}
}
