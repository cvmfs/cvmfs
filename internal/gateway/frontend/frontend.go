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

	// Register the different routes

	// Root handler
	router.Path(APIRoot).HandlerFunc(NewRootHandler())

	// Repositories
	router.Path(APIRoot + "/repos/{name}").
		Methods("GET").
		HandlerFunc(NewGetReposHandler(&services.Access))
	router.Path(APIRoot + "/repos").
		Methods("GET").
		HandlerFunc(NewGetReposHandler(&services.Access))

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
