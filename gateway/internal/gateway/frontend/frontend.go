package frontend

import (
	"fmt"
	"net/http"
	"time"

	be "github.com/cvmfs/gateway/internal/gateway/backend"

	"github.com/julienschmidt/httprouter"
)

type frontendConfig struct {
	enableKeyEndpoint bool
}

// FrontendOption configures optional frontend behaviour
type FrontendOption func(*frontendConfig)

// WithKeyEndpoint enables the /repos/:name/keys endpoint
func WithKeyEndpoint(enable bool) FrontendOption {
	return func(c *frontendConfig) { c.enableKeyEndpoint = enable }
}

// NewFrontend builds and configures a new HTTP server, but does not start it
func NewFrontend(services be.ActionController, port int, timeout time.Duration, opts ...FrontendOption) *http.Server {
	cfg := frontendConfig{}
	for _, o := range opts {
		o(&cfg)
	}

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
	router.PATCH(APIRoot+"/leases/:token", mw(MakeLeasesHandler(services)))
	// EXPERIMENTAL DirectGraft endpoint; keep separate from stable commits.
	router.POST(APIRoot+"/leases/:token/graft", mw(MakeGraftHandler(services)))
	router.DELETE(APIRoot+"/leases/:token", mw(MakeLeasesHandler(services)))

	// Payloads (legacy endpoint)
	router.POST(APIRoot+"/payloads", mw(MakePayloadsHandler(services)))
	// Payloads (new and improved)
	router.POST(APIRoot+"/payloads/:token", mw(MakePayloadsHandler(services)))

	// Notification system endpoints
	router.POST(APIRoot+"/notifications/publish", tag(MakeNotificationsHandler(services)))
	router.GET(APIRoot+"/notifications/subscribe", tag(MakeNotificationsHandler(services)))

	// Repository keys (opt-in endpoint for publisher setup, HMAC-authenticated)
	router.GET(APIRoot+"/repos/:name/keys", mw(MakeRepoKeysHandler(services, cfg.enableKeyEndpoint)))

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
func Start(services *be.Services, port int, timeout time.Duration, opts ...FrontendOption) error {
	srv := NewFrontend(services, port, timeout, opts...)
	if err := srv.ListenAndServe(); err != nil {
		return fmt.Errorf("could not run HTTP front-end: %w", err)
	}

	return nil
}
