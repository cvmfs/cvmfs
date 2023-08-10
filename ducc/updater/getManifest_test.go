package updater

import (
	"encoding/json"
	"io"
	"net/http"
	"os"
	"testing"

	"github.com/cvmfs/ducc/db"
	"github.com/cvmfs/ducc/test"
)

func TestFetchManifest(t *testing.T) {
	stop := make(chan interface{})
	ready := make(chan interface{})
	go mockManifestServer(stop, ready)
	defer close(stop)

	<-ready

	image := db.Image{
		RegistryScheme: "http",
		RegistryHost:   "localhost:8080",
		Repository:     "library/busybox",
		Tag:            "latest",
	}

	manifest, err := fetchAndParseManifest(image)
	if err != nil {
		t.Fatal(err)
	}
	parsedMaifestJSON, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		t.Fatal(err)
	}

	test.CompareWithGoldenFile(t, "testdata/fetch_manifest.golden.json", string(parsedMaifestJSON), *test.UpdateTests)
}

func mockManifestServer(stop <-chan interface{}, ready chan<- interface{}) {
	f, err := os.Open("testdata/example_manifest.json")
	if err != nil {
		panic(err)
	}
	manifest, err := io.ReadAll(f)
	if err != nil {
		panic(err)
	}
	f.Close()

	handler := func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/vnd.docker.distribution.manifest.v2+json")
		w.Write(manifest)
		w.WriteHeader(http.StatusOK)
	}

	srv := &http.Server{Addr: ":8080", Handler: http.HandlerFunc(handler)}
	go srv.ListenAndServe()
	close(ready)
	go func() {
		<-stop
		srv.Close()
	}()
}
