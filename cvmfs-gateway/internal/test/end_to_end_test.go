package test

/*

import (
	"net/http"
	"os"
	"testing"
	"time"

	be "github.com/cvmfs/gateway/internal/gateway/backend"
	fe "github.com/cvmfs/gateway/internal/gateway/frontend"
)

const (
	testServerURLRoot = "http://localhost:4929/api/v1"
)

func startTestServer() (*http.Server, *be.Services) {
	backend := be.StartTestBackend("end_to_end_test", 1*time.Second)

	srv := fe.NewFrontend(
		backend, backend.Config.Port, backend.Config.MaxLeaseTime)

	go func() {
		if err := srv.ListenAndServe(); err != nil {
			os.Exit(1)
		}
	}()

	time.Sleep(50 * time.Millisecond)

	return srv, backend
}

func TestMain(m *testing.M) {
	srv, backend := startTestServer()
	defer func() { srv.Close(); backend.Stop() }()

	os.Exit(m.Run())
}

func TestEndToEndFullSession(t *testing.T) {
	_, err := http.DefaultClient.Get(testServerURLRoot)
	if err != nil {
		t.Fatalf("could not make GET request to API root")
	}
}
*/
