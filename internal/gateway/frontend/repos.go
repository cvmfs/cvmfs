package frontend

import (
	"net/http"
	"time"

	gw "github.com/cvmfs/gateway/internal/gateway"
	be "github.com/cvmfs/gateway/internal/gateway/backend"
	"github.com/google/uuid"
	"github.com/gorilla/mux"
)

// MakeReposHandler creates an HTTP handler for the API root
func MakeReposHandler(services *be.Services) http.HandlerFunc {
	return func(w http.ResponseWriter, h *http.Request) {
		reqID, _ := h.Context().Value(idKey).(uuid.UUID)
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

		t0, _ := h.Context().Value(t0Key).(time.Time)
		gw.Log.Debug().
			Str("component", "http").
			Str("req_id", reqID.String()).
			Float64("duration", time.Since(t0).Seconds()).
			Msg("request processed")

		replyJSON(&reqID, w, msg)
	}
}
