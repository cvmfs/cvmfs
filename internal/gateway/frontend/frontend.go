package frontend

import (
	"fmt"
	"net/http"
	"time"

	be "github.com/cvmfs/gateway/internal/gateway/backend"

	"github.com/gorilla/mux"
	"github.com/pkg/errors"
)

// Start HTTP frontend
func Start(services *be.Services, port int, maxLeaseTime int) error {
	router := mux.NewRouter()

	// Registers the different routes

	r := router.NewRoute()
	r.Path(APIRoot + "/")
	r.HandlerFunc(NewRootHandler())

	r = router.NewRoute()
	r.Path(APIRoot + "/repos")
	r.Methods("GET")
	r.HandlerFunc(NewGetReposHandler())

	// Configure and start the HTTP server
	timeout := time.Duration(maxLeaseTime) * time.Second
	srv := &http.Server{
		Handler:      router,
		Addr:         fmt.Sprintf(":%d", port),
		WriteTimeout: timeout,
		ReadTimeout:  timeout,
	}

	if err := srv.ListenAndServe(); err != nil {
		return errors.Wrap(err, "could not run HTTP front-end")
	}

	return nil
}
