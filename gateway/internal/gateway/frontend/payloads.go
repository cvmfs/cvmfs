package frontend

import (
	"net/http"
	"time"

	gw "github.com/cvmfs/gateway/internal/gateway"
	be "github.com/cvmfs/gateway/internal/gateway/backend"
	"github.com/google/uuid"
	"github.com/gorilla/mux"
)

// MakePayloadsHandler creates an HTTP handler for the API root
func MakePayloadsHandler(services *be.Services) http.HandlerFunc {
	return func(w http.ResponseWriter, h *http.Request) {
		token, hasArg := mux.Vars(h)["token"]
		if hasArg {
			handlePayload(services, token, w, h)
		} else {
			handlePayloadLegacy(services, w, h)
		}
	}
}

func handlePayload(services *be.Services, token string, w http.ResponseWriter, h *http.Request) {
	reqID, _ := h.Context().Value(idKey).(uuid.UUID)

	gw.Log.Debug().
		Str("component", "http").
		Str("req_id", reqID.String()).
		Msg("brave new world")

	t0, _ := h.Context().Value(t0Key).(time.Time)
	gw.Log.Debug().
		Str("component", "http").
		Str("req_id", reqID.String()).
		Float64("time", time.Since(t0).Seconds()).
		Msg("request processed")
	http.Error(w, "not implemented", http.StatusNotImplemented)
}

func handlePayloadLegacy(services *be.Services, w http.ResponseWriter, h *http.Request) {
	reqID, _ := h.Context().Value(idKey).(uuid.UUID)

	gw.Log.Debug().
		Str("component", "http").
		Str("req_id", reqID.String()).
		Msg("legacy")

	t0, _ := h.Context().Value(t0Key).(time.Time)
	gw.Log.Debug().
		Str("component", "http").
		Str("req_id", reqID.String()).
		Float64("time", time.Since(t0).Seconds()).
		Msg("request processed")
	http.Error(w, "not implemented", http.StatusNotImplemented)
}
