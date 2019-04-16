package frontend

import (
	"fmt"
	"net/http"

	"github.com/google/uuid"
)

// NewRootHandler creates an HTTP handler for the API root
func NewRootHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, h *http.Request) {
		reqID, _ := h.Context().Value(idKey).(uuid.UUID)
		msg := make(map[string]interface{})
		msg["welcome"] = fmt.Sprintf(
			"You are in an open field on the west side " +
				"of a white house with a boarded front door.")
		replyJSON(&reqID, w, msg)
	}
}
