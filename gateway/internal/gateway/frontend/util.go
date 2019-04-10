package frontend

import (
	"net/http"

	gw "github.com/cvmfs/gateway/internal/gateway"
)

func httpWrapError(err error, msg string, w *http.ResponseWriter, code int) {
	gw.Log.Error().Err(err).Msg(msg)
	http.Error(*w, msg, code)
}
