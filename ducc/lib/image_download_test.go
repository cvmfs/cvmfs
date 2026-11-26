package lib

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/registry"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/static"
	"github.com/google/go-containerregistry/pkg/v1/types"
)

func TestGetLayersReportsDownloadErrors(t *testing.T) {
	srv := httptest.NewServer(registry.New())
	defer srv.Close()
	host := strings.TrimPrefix(srv.URL, "http://")

	// gzip magic followed by an invalid header: every download attempt fails
	broken := static.NewLayer([]byte{0x1f, 0x8b, 0x00, 0x00, 0x00, 0x00}, types.OCILayer)
	img, err := mutate.AppendLayers(mutate.MediaType(empty.Image, types.OCIManifestSchema1), broken)
	if err != nil {
		t.Fatal(err)
	}
	tag, err := name.NewTag(host+"/ducc-test/broken:latest", name.Insecure)
	if err != nil {
		t.Fatal(err)
	}
	if err := remote.Write(tag, img); err != nil {
		t.Fatal(err)
	}

	image, err := ParseImage(srv.URL + "/ducc-test/broken:latest")
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := image.GetManifest()
	if err != nil {
		t.Fatal(err)
	}

	layersChan := make(chan downloadedLayer, len(manifest.Layers))
	manifestChan := make(chan string, 1)
	stop := make(chan bool, 1)
	err = image.GetLayersWithLogger(nil, manifest, layersChan, manifestChan, stop,
		t.TempDir(), 0, "ducc-unit-test.invalid", false)
	if err == nil {
		t.Fatal("expected an error for a layer that cannot be downloaded")
	}
	for range layersChan {
		t.Error("no layer should have been sent")
	}
}
