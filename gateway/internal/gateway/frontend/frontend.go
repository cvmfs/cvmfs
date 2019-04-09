package frontend

import (
	"fmt"
	"net/http"
	"time"

	"github.com/gorilla/mux"
	"github.com/pkg/errors"
)

const apiRoot = "/api/v1"

// Start HTTP frontend
func Start(port int, maxLeaseTime int) error {
	router := mux.NewRouter()

	r := router.NewRoute()
	r.Path(APIRoot + "/")
	r.HandlerFunc(NewRootHandler())

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
