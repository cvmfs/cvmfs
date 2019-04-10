package frontend

import (
	"encoding/json"
	"net/http"

	be "github.com/cvmfs/gateway/internal/gateway/backend"
	"github.com/gorilla/mux"
)

// NewGetReposHandler creates an HTTP handler for the API root
func NewGetReposHandler(ac *be.AccessConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, h *http.Request) {
		vs := mux.Vars(h)

		var rep []byte
		var err error
		if repoName, present := vs["name"]; present {
			rep, err = json.Marshal(ac.Repositories[repoName])
		} else {
			rep, err = json.Marshal(ac.Repositories)
		}
		if err != nil {
			httpWrapError(err, "JSON serialization failed", &w, http.StatusInternalServerError)
			return
		}

		w.Write(rep)
	}
}
