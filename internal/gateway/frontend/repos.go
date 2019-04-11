package frontend

import (
	"net/http"

	be "github.com/cvmfs/gateway/internal/gateway/backend"
	"github.com/gorilla/mux"
)

// MakeReposHandler creates an HTTP handler for the API root
func MakeReposHandler(services *be.Services) http.HandlerFunc {
	return func(w http.ResponseWriter, h *http.Request) {
		vs := mux.Vars(h)

		msg := make(map[string]interface{})

		if repoName, present := vs["name"]; present {
			r := services.Access.GetRepo(repoName)
			if len(r) == 0 {
				msg["status"] = "error"
				msg["reason"] = "invalid_repo"
			} else {
				msg["status"] = "ok"
				msg["data"] = r
			}
		} else {
			msg["status"] = "ok"
			msg["data"] = services.Access.GetRepos()
		}

		replyJSON(w, msg)
	}
}
