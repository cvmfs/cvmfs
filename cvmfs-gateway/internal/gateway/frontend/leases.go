package frontend

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	gw "github.com/cvmfs/gateway/internal/gateway"
	be "github.com/cvmfs/gateway/internal/gateway/backend"
	"github.com/julienschmidt/httprouter"
)

// MakeLeasesHandler creates an HTTP handler for the API root
func MakeLeasesHandler(services be.ActionController) httprouter.Handle {
	return func(w http.ResponseWriter, h *http.Request, ps httprouter.Params) {
		token := ps.ByName("token")
		switch h.Method {
		case "GET":
			handleGetLeases(services, token, w, h)
		case "POST":
			if token != "" {
				// Committing an existing lease (transaction)
				handleCommitLease(services, token, w, h)
			} else {
				// Requesting a new lease
				handleNewLease(services, w, h)
			}
		case "DELETE":
			handleCancelLease(services, token, w, h)
		default:
			gw.LogC(h.Context(), "http", gw.LogError).
				Msgf("invalid HTTP method: %v", h.Method)
			http.Error(w, "invalid method", http.StatusNotFound)
			return
		}
		gw.LogC(h.Context(), "http", gw.LogInfo).Msg("request processed")
	}
}

func handleGetLeases(services be.ActionController, token string, w http.ResponseWriter, h *http.Request) {
	ctx := h.Context()
	msg := make(map[string]interface{})
	if token == "" {
		leases, err := services.GetLeases(ctx)
		if err != nil {
			httpWrapError(ctx, err, err.Error(), w, http.StatusInternalServerError)
			return
		}
		msg["status"] = "ok"
		msg["data"] = leases
	} else {
		lease, err := services.GetLease(ctx, token)
		if err != nil {
			httpWrapError(ctx, err, err.Error(), w, http.StatusInternalServerError)
			return
		}
		msg["data"] = lease
	}

	replyJSON(ctx, w, msg)
}

func handleNewLease(services be.ActionController, w http.ResponseWriter, h *http.Request) {
	ctx := h.Context()

	var reqMsg struct {
		Path    string `json:"path"`
		Version string `json:"api_version"` // cvmfs_swissknife sends this field as a string
	}
	if err := json.NewDecoder(h.Body).Decode(&reqMsg); err != nil {
		httpWrapError(ctx, err, "invalid request body", w, http.StatusBadRequest)
		return
	}

	clientVersion, err := strconv.Atoi(reqMsg.Version)
	if err != nil {
		httpWrapError(ctx, err, "invalid request body", w, http.StatusBadRequest)
		return
	}

	msg := make(map[string]interface{})
	if clientVersion < MinAPIProtocolVersion {
		msg["status"] = "error"
		msg["reason"] = fmt.Sprintf(
			"incompatible request version: %v, min version: %v",
			clientVersion,
			MinAPIProtocolVersion)
	} else {
		// The authorization is expected to have the correct format, since it has already been checked.
		keyID := strings.Split(h.Header.Get("Authorization"), " ")[0]
		protocolVersion := MaxAPIVersion(clientVersion)
		token, err := services.NewLease(ctx, keyID, reqMsg.Path, protocolVersion)
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
			msg["max_api_version"] = protocolVersion
		}
	}

	replyJSON(ctx, w, msg)
}

func handleCommitLease(services be.ActionController, token string, w http.ResponseWriter, h *http.Request) {
	ctx := h.Context()

	var reqMsg struct {
		OldRootHash string `json:"old_root_hash"`
		NewRootHash string `json:"new_root_hash"`
		gw.RepositoryTag
	}
	if err := json.NewDecoder(h.Body).Decode(&reqMsg); err != nil {
		httpWrapError(ctx, err, "invalid request body", w, http.StatusBadRequest)
		return
	}

	msg := make(map[string]interface{})
	if finalRev, err := services.CommitLease(
		ctx, token, reqMsg.OldRootHash, reqMsg.NewRootHash, reqMsg.RepositoryTag); err != nil {
		msg["status"] = "error"
		msg["reason"] = err.Error()
	} else {
		msg["status"] = "ok"
		msg["final_revision"] = finalRev
	}

	replyJSON(ctx, w, msg)
}

func handleCancelLease(services be.ActionController, token string, w http.ResponseWriter, h *http.Request) {
	if token == "" {
		http.Error(w, "missing token", http.StatusBadRequest)
		return
	}

	ctx := h.Context()

	msg := make(map[string]interface{})

	if err := services.CancelLease(ctx, token); err != nil {
		msg["status"] = "error"
		if _, ok := err.(be.InvalidTokenError); ok {
			msg["reason"] = "invalid_token"
		} else {
			msg["reason"] = err.Error()
		}

	} else {
		msg["status"] = "ok"
	}

	replyJSON(ctx, w, msg)
}
