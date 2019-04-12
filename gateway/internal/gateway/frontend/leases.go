package frontend

import (
	"encoding/json"
	"net/http"

	gw "github.com/cvmfs/gateway/internal/gateway"
	be "github.com/cvmfs/gateway/internal/gateway/backend"
	"github.com/google/uuid"
	"github.com/gorilla/mux"
)

// MakeLeasesHandler creates an HTTP handler for the API root
func MakeLeasesHandler(services *be.Services) http.HandlerFunc {
	return func(w http.ResponseWriter, h *http.Request) {
		switch h.Method {
		case "GET":
			handleGetLeases(services, w, h)
		case "POST":
			vs := mux.Vars(h)
			if token, present := vs["token"]; present {
				// Committing an existing lease (transaction)
				handleCommitLease(services, token, w, h)
			} else {
				// Requesting a new lease
				handleNewLease(services, w, h)
			}
		case "DELETE":
			handleDropLease(services, w, h)
		default:
			reqID, _ := h.Context().Value(idKey).(uuid.UUID)
			gw.Log.Error().
				Str("component", "http").
				Str("req_id", reqID.String()).
				Msgf("invalid HTTP method: %v", h.Method)
			http.Error(w, "invalid method", http.StatusNotFound)
		}
	}
}

func handleGetLeases(services *be.Services, w http.ResponseWriter, h *http.Request) {
	http.Error(w, "not implemented", http.StatusNotImplemented)
}

func handleNewLease(services *be.Services, w http.ResponseWriter, h *http.Request) {
	reqID, _ := h.Context().Value(idKey).(uuid.UUID)

	var reqMsg struct {
		Path    string `json:"path"`
		Version int    `json:"api_version"`
	}
	if err := json.NewDecoder(h.Body).Decode(&reqMsg); err != nil {
		httpWrapError(&reqID, err, "invalid request body", w, http.StatusBadRequest)
	}
}

func handleCommitLease(services *be.Services, token string, w http.ResponseWriter, h *http.Request) {
	http.Error(w, "not implemented", http.StatusNotImplemented)
}

func handleDropLease(services *be.Services, w http.ResponseWriter, h *http.Request) {
	http.Error(w, "not implemented", http.StatusNotImplemented)
}
