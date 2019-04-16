package frontend

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	gw "github.com/cvmfs/gateway/internal/gateway"
	be "github.com/cvmfs/gateway/internal/gateway/backend"
	"github.com/google/uuid"
	"github.com/gorilla/mux"
)

// MakeLeasesHandler creates an HTTP handler for the API root
func MakeLeasesHandler(services *be.Services) http.HandlerFunc {
	return func(w http.ResponseWriter, h *http.Request) {
		vs := mux.Vars(h)
		token, hasArg := vs["arg"]
		switch h.Method {
		case "GET":
			handleGetLeases(services, token, w, h)
		case "POST":
			if hasArg {
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

type leaseReturn struct {
	KeyID     string `json:"key_id,omitempty"`
	LeasePath string `json:"path,omitempty"`
	TokenStr  string `json:"token,omitempty"`
	Expires   string `json:"expires,omitempty"`
}

func handleGetLeases(services *be.Services, token string, w http.ResponseWriter, h *http.Request) {
	reqID, _ := h.Context().Value(idKey).(uuid.UUID)
	msg := make(map[string]interface{})
	if token == "" {
		leases, err := services.Leases.GetLeases()
		if err != nil {
			httpWrapError(&reqID, err, err.Error(), w, http.StatusInternalServerError)
		}
		msg["status"] = "ok"
		r := make(map[string]interface{})
		for k, v := range leases {
			r[k] = leaseReturn{KeyID: v.KeyID, TokenStr: v.Token.TokenStr, Expires: v.Token.Expiration.String()}
		}
		msg["data"] = r
	} else {
		leasePath, lease, err := services.Leases.GetLeaseForToken(token)
		if err != nil {
			httpWrapError(&reqID, err, err.Error(), w, http.StatusInternalServerError)
		}
		msg["data"] = leaseReturn{
			KeyID:     lease.KeyID,
			LeasePath: leasePath,
			Expires:   lease.Token.Expiration.String(),
		}
	}

	gw.Log.Debug().
		Str("component", "http").
		Str("req_id", reqID.String()).
		Float64("duration", time.Since(h.Context().Value(t0Key).(time.Time)).Seconds()).
		Msg("request processed")

	replyJSON(&reqID, w, msg)
}

func handleNewLease(services *be.Services, w http.ResponseWriter, h *http.Request) {
	reqID, _ := h.Context().Value(idKey).(uuid.UUID)

	var reqMsg struct {
		Path    string `json:"path"`
		Version int    `json:"api_version"`
	}
	if err := json.NewDecoder(h.Body).Decode(&reqMsg); err != nil {
		httpWrapError(&reqID, err, "invalid request body", w, http.StatusBadRequest)
		return
	}

	msg := make(map[string]interface{})
	if reqMsg.Version < MinAPIProtocolVersion() {
		msg["status"] = "error"
		msg["reason"] = fmt.Sprintf(
			"incompatible request version: %v, min version: %v",
			reqMsg.Version,
			MinAPIProtocolVersion())
	} else {
		// The authorization is expected to have the correct format, since it has already been checked.
		keyID := strings.Split(h.Header.Get("Authorization"), " ")[0]
		token, err := services.RequestNewLease(keyID, reqMsg.Path)
		if err != nil {
			if busyError, ok := err.(be.PathBusyError); ok {
				msg["status"] = "path_busy"
				msg["time_remaining"] = busyError.Remaining().String()
			} else {
				msg["status"] = "error"
				msg["reason"] = err.Error()
			}
		} else {
			msg["status"] = "ok"
			msg["session_token"] = token
			msg["max_api_version"] = MinAPIProtocolVersion()
		}
	}

	gw.Log.Debug().
		Str("component", "http").
		Str("req_id", reqID.String()).
		Float64("duration", time.Since(h.Context().Value(t0Key).(time.Time)).Seconds()).
		Msg("request processed")

	replyJSON(&reqID, w, msg)
}

func handleCommitLease(services *be.Services, token string, w http.ResponseWriter, h *http.Request) {
	http.Error(w, "not implemented", http.StatusNotImplemented)
}

func handleDropLease(services *be.Services, w http.ResponseWriter, h *http.Request) {
	http.Error(w, "not implemented", http.StatusNotImplemented)
}
