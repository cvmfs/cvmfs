package frontend

import (
	"encoding/json"
	"net/http"

	gw "github.com/cvmfs/gateway/internal/gateway"
	"github.com/google/uuid"
)

// private type alias for context keys
type ctxKey int

const (
	idKey ctxKey = iota
	t0Key
)

type message map[string]interface{}

func replyJSON(reqID *uuid.UUID, w http.ResponseWriter, msg message) {
	rep, err := json.Marshal(msg)
	if err != nil {
		httpWrapError(reqID, err, "JSON serialization failed", w, http.StatusInternalServerError)
		return
	}
	w.Write(rep)
}

func httpWrapError(reqID *uuid.UUID, err error, msg string, w http.ResponseWriter, code int) {
	gw.Log.Error().
		Str("component", "http").
		Str("req_id", reqID.String()).
		Err(err).Msg(msg)
	http.Error(w, msg, code)
}
