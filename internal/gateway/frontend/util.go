package frontend

import (
	"encoding/json"
	"net/http"

	gw "github.com/cvmfs/gateway/internal/gateway"
)

func httpWrapError(err error, msg string, w http.ResponseWriter, code int) {
	gw.Log.Error().Err(err).Msg(msg)
	http.Error(w, msg, code)
}

type message map[string]interface{}

func replyJSON(w http.ResponseWriter, msg message) {
	rep, err := json.Marshal(msg)
	if err != nil {
		httpWrapError(err, "JSON serialization failed", w, http.StatusInternalServerError)
		return
	}
	w.Write(rep)
}
