package testutils

import (
	"archive/tar"
	"bytes"
	"context"
	"fmt"

	"github.com/google/go-containerregistry/pkg/compression"
	"github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/static"
	"github.com/google/go-containerregistry/pkg/v1/tarball"
	"github.com/google/go-containerregistry/pkg/v1/types"
)

// LayerCompressions lists the layer encodings found in the wild that ducc must
// ingest. The keys are used as repository name suffixes.
var LayerCompressions = []string{"gzip", "zstd", "uncompressed"}

// CompressionTestFiles maps the files in each compression test image to their
// content, one file per layer.
var CompressionTestFiles = map[string]string{
	"base/README.txt":     "base layer",
	"app/hello.txt":       "hello from the second layer",
	"app/nested/data.txt": "third layer",
}

func tarBytes(path, content string) ([]byte, error) {
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	if err := tw.WriteHeader(&tar.Header{Name: path, Mode: 0644, Size: int64(len(content))}); err != nil {
		return nil, err
	}
	if _, err := tw.Write([]byte(content)); err != nil {
		return nil, err
	}
	if err := tw.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func compressedLayer(raw []byte, kind string) (v1.Layer, error) {
	switch kind {
	case "gzip":
		return tarball.LayerFromReader(bytes.NewReader(raw), tarball.WithMediaType(types.OCILayer))
	case "zstd":
		return tarball.LayerFromReader(bytes.NewReader(raw),
			tarball.WithCompression(compression.ZStd),
			tarball.WithMediaType(types.OCILayerZStd))
	case "uncompressed":
		return static.NewLayer(raw, types.OCIUncompressedLayer), nil
	}
	return nil, fmt.Errorf("unknown layer compression %q", kind)
}

// CreateCompressionTestImage builds an OCI image whose layers all use the
// given compression (one of LayerCompressions).
func CreateCompressionTestImage(kind string) (v1.Image, error) {
	var layers []v1.Layer
	for _, path := range []string{"base/README.txt", "app/hello.txt", "app/nested/data.txt"} {
		raw, err := tarBytes(path, CompressionTestFiles[path])
		if err != nil {
			return nil, fmt.Errorf("build tar for %s: %w", path, err)
		}
		layer, err := compressedLayer(raw, kind)
		if err != nil {
			return nil, err
		}
		layers = append(layers, layer)
	}
	img := mutate.MediaType(empty.Image, types.OCIManifestSchema1)
	img = mutate.ConfigMediaType(img, types.OCIConfigJSON)
	// Same files in every variant give the same diff IDs, hence the same
	// config digest and the same flat image path; a label keeps them apart.
	img, err := mutate.Config(img, v1.Config{Labels: map[string]string{"ducc-test-compression": kind}})
	if err != nil {
		return nil, err
	}
	return mutate.AppendLayers(img, layers...)
}

// PushCompressionTestImages pushes one image per entry of LayerCompressions to
// <host>/<repoPrefix>-<compression>:latest.
func PushCompressionTestImages(ctx context.Context, host, repoPrefix string) error {
	for _, kind := range LayerCompressions {
		img, err := CreateCompressionTestImage(kind)
		if err != nil {
			return fmt.Errorf("create %s image: %w", kind, err)
		}
		ref := fmt.Sprintf("%s/%s-%s:latest", host, repoPrefix, kind)
		tag, err := name.NewTag(ref, name.Insecure)
		if err != nil {
			return fmt.Errorf("parse tag %q: %w", ref, err)
		}
		if err := remote.Write(tag, img, remote.WithContext(ctx)); err != nil {
			return fmt.Errorf("push %s: %w", ref, err)
		}
	}
	return nil
}
