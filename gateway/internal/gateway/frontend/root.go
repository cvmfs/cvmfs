package frontend

import (
	"fmt"
	"net/http"
)

// NewRootHandler creates an HTTP handler for the API root
func NewRootHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, h *http.Request) {
		r := fmt.Sprintf(
			"You are in an open field on the west side " +
				"of a white house with a boarded front door.\n")
		w.Write([]byte(r))
	}
}
