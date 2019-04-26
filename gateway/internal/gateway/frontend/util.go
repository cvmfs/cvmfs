package frontend

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	gw "github.com/cvmfs/gateway/internal/gateway"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
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

func frontendLog(ctx context.Context, level gw.LogLevel, format string, args ...interface{}) {
	reqID, _ := ctx.Value(idKey).(uuid.UUID)
	t0, _ := ctx.Value(t0Key).(time.Time)

	var event *zerolog.Event
	switch level {
	case gw.DebugLevel:
		event = gw.Log.Debug()
	case gw.InfoLevel:
		event = gw.Log.Info()
	case gw.ErrorLevel:
		event = gw.Log.Error()
	default:
		gw.Log.Error().
			Str("component", "logger").
			Msgf("unknown log level: %v", level)
		return
	}
	event.
		Str("component", "http").
		Str("req_id", reqID.String()).
		Float64("time", time.Since(t0).Seconds()).
		Msgf(format, args...)
}
