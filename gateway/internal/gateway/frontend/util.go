package frontend

import (
	"context"
	"encoding/json"
	"net/http"

	gw "github.com/cvmfs/gateway/internal/gateway"
)

func replyJSON(ctx context.Context, w http.ResponseWriter, msg message) {
	rep, err := json.Marshal(msg)
	if err != nil {
		httpWrapError(ctx, err, "JSON serialization failed", w, http.StatusInternalServerError)
		return
	}
	w.Write(rep)
}

func httpWrapError(ctx context.Context, err error, msg string, w http.ResponseWriter, code int) {
	gw.LogC(ctx, "http", gw.LogError).Err(err).Msg(msg)
	http.Error(w, msg, code)
}
