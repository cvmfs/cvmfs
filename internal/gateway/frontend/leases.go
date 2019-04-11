package frontend

import (
	"net/http"

	be "github.com/cvmfs/gateway/internal/gateway/backend"
)

// MakeLeasesHandler creates an HTTP handler for the API root
func MakeLeasesHandler(services *be.Services) http.HandlerFunc {
	return func(w http.ResponseWriter, h *http.Request) {
	}
}
