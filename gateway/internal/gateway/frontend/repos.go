package frontend

import "net/http"

// NewGetReposHandler creates an HTTP handler for the API root
func NewGetReposHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, h *http.Request) {
	}
}
