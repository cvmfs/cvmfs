package testutils

import (
	"bytes"
	"io"
	"testing"

	"github.com/google/go-containerregistry/pkg/v1/types"
)

func TestCompressionTestImageEncodings(t *testing.T) {
	wantMediaType := map[string]types.MediaType{
		"gzip":         types.OCILayer,
		"zstd":         types.OCILayerZStd,
		"uncompressed": types.OCIUncompressedLayer,
	}
	wantMagic := map[string][]byte{
		"gzip": {0x1f, 0x8b},
		"zstd": {0x28, 0xb5, 0x2f, 0xfd},
		// a tar stream starts with the first member's name
		"uncompressed": []byte("base/README.txt"),
	}
	for _, kind := range LayerCompressions {
		img, err := CreateCompressionTestImage(kind)
		if err != nil {
			t.Fatalf("%s: %v", kind, err)
		}
		layers, err := img.Layers()
		if err != nil {
			t.Fatalf("%s: %v", kind, err)
		}
		if len(layers) != len(CompressionTestFiles) {
			t.Fatalf("%s: got %d layers, want %d", kind, len(layers), len(CompressionTestFiles))
		}
		mt, _ := layers[0].MediaType()
		if mt != wantMediaType[kind] {
			t.Errorf("%s: media type %s, want %s", kind, mt, wantMediaType[kind])
		}
		rc, err := layers[0].Compressed()
		if err != nil {
			t.Fatalf("%s: %v", kind, err)
		}
		blob, _ := io.ReadAll(rc)
		rc.Close()
		if !bytes.HasPrefix(blob, wantMagic[kind]) {
			t.Errorf("%s: blob starts with % x", kind, blob[:8])
		}
	}
}
