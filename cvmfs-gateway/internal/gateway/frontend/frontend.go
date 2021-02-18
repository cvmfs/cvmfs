package frontend

import (
	"fmt"
	"net/http"
	"time"

	be "github.com/cvmfs/gateway/internal/gateway/backend"

	"github.com/julienschmidt/httprouter"
	"github.com/pkg/errors"
)

// NewFrontend builds and configures a new HTTP server, but does not start it
func NewFrontend(services be.ActionController, port int, timeout time.Duration) *http.Server {
	router := httprouter.New()

	// middleware which only tags requests for GET
	tag := func(h httprouter.Handle) httprouter.Handle {
		return WithTag(h)
	}

	// middleware which tags requests and performs HMAC authorization
	mw := func(h httprouter.Handle) httprouter.Handle {
		return WithTag(WithAuthz(services, h))
	}

	// middleware with tagging and admin authorization
	amw := func(h httprouter.Handle) httprouter.Handle {
		return WithTag(WithAdminAuthz(services, h))
	}

	// Regular routes

	// Root handler
	router.GET(APIRoot, tag(NewRootHandler()))

	// Repositories
	router.GET(APIRoot+"/repos", tag(MakeReposHandler(services)))
	router.GET(APIRoot+"/repos/:name", tag(MakeReposHandler(services)))

	// Leases
	router.GET(APIRoot+"/leases", tag(MakeLeasesHandler(services)))
	router.GET(APIRoot+"/leases/:token", tag(MakeLeasesHandler(services)))
	router.POST(APIRoot+"/leases", mw(MakeLeasesHandler(services)))
	router.POST(APIRoot+"/leases/:token", mw(MakeLeasesHandler(services)))
	router.DELETE(APIRoot+"/leases/:token", mw(MakeLeasesHandler(services)))

	// Payloads (legacy endpoint)
	router.POST(APIRoot+"/payloads", mw(MakePayloadsHandler(services)))
	// Payloads (new and improved)
	router.POST(APIRoot+"/payloads/:token", mw(MakePayloadsHandler(services)))

	// Notification system endpoints
	router.POST(APIRoot+"/notifications/publish", tag(MakeNotificationsHandler(services)))
	router.GET(APIRoot+"/notifications/subscribe", tag(MakeNotificationsHandler(services)))

	// Admin routes
	router.POST(APIRoot+"/repos/:name", amw(MakeAdminReposHandler(services)))
	router.DELETE(APIRoot+"/leases-by-path/*path", amw(MakeAdminLeasesHandler(services)))
	router.POST(APIRoot+"/gc", amw(MakeGCHandler(services)))

	// Configure and start the HTTP server
	srv := &http.Server{
		Handler:      router,
		Addr:         fmt.Sprintf(":%d", port),
		WriteTimeout: timeout,
		ReadTimeout:  timeout,
	}

	return srv
}

// Start HTTP frontend
func Start(services *be.Services, port int, timeout time.Duration) error {
	srv := NewFrontend(services, port, timeout)
	if err := srv.ListenAndServe(); err != nil {
		return errors.Wrap(err, "could not run HTTP front-end")
	}

	return nil
}
