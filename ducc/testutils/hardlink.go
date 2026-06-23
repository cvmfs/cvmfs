package testutils

// Helpers to build and push an OCI image whose layer contains a *dangling*
// hardlink: a tar hardlink entry (TypeLink) whose target is not a member of the
// same layer.  This reproduces the cross-layer / cleaned-cache hardlinks found
// in real images (e.g. `dnf clean`-style layers) that crash `cvmfs_server
// ingest` unless --tolerate-missing-hardlinks is used.

import (
	"archive/tar"
	"bytes"
	"context"
	"fmt"
	"io"
	"time"

	"github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/tarball"
)

// Paths used by the crafted dangling-hardlink layer.  DanglingHardlinkLinkPath
// is the hardlink whose target (DanglingHardlinkTargetPath) is deliberately
// absent from the layer's tar.
const (
	DanglingHardlinkRegularPath = "usr/share/ducc-test/data.txt"
	DanglingHardlinkLinkPath    = "var/cache/dnf/x86_64/repodata/comps.xml.gz"
	DanglingHardlinkTargetPath  = "var/cache/dnf/noarch/repodata/comps.xml.gz"
)

// CreateDanglingHardlinkLayer builds a single image layer that contains one
// real regular file plus a hardlink entry pointing at a path that is NOT
// present in the layer (a dangling/cross-layer hardlink).
func CreateDanglingHardlinkLayer() (v1.Layer, error) {
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)

	content := []byte("regular file so the layer carries real content\n")
	if err := tw.WriteHeader(&tar.Header{
		Typeflag: tar.TypeReg,
		Name:     DanglingHardlinkRegularPath,
		Mode:     0644,
		Size:     int64(len(content)),
		ModTime:  time.Unix(0, 0),
	}); err != nil {
		return nil, fmt.Errorf("write regular header: %w", err)
	}
	if _, err := tw.Write(content); err != nil {
		return nil, fmt.Errorf("write regular content: %w", err)
	}

	// The dangling hardlink: TypeLink with a Linkname that is not a member of
	// this archive.  Hardlink entries carry no data (Size 0).
	if err := tw.WriteHeader(&tar.Header{
		Typeflag: tar.TypeLink,
		Name:     DanglingHardlinkLinkPath,
		Linkname: DanglingHardlinkTargetPath,
		Mode:     0644,
		ModTime:  time.Unix(0, 0),
	}); err != nil {
		return nil, fmt.Errorf("write hardlink header: %w", err)
	}

	if err := tw.Close(); err != nil {
		return nil, fmt.Errorf("close tar: %w", err)
	}

	return tarball.LayerFromReader(&buf)
}

// CreateDanglingHardlinkImage builds an image with the dangling-hardlink layer.
func CreateDanglingHardlinkImage() (v1.Image, error) {
	layer, err := CreateDanglingHardlinkLayer()
	if err != nil {
		return nil, err
	}
	img, err := mutate.AppendLayers(empty.Image, layer)
	if err != nil {
		return nil, fmt.Errorf("append layer: %w", err)
	}
	cfg, err := img.ConfigFile()
	if err != nil {
		return nil, err
	}
	cfg.Created = v1.Time{Time: time.Unix(0, 0)}
	cfg.Author = "ducc dangling-hardlink test"
	cfg.Config.Cmd = []string{"/bin/true"}
	return mutate.ConfigFile(img, cfg)
}

// PushDanglingHardlinkImage builds and pushes the dangling-hardlink image to
// the given (insecure, http) registry reference, e.g.
// "localhost:5000/ducc-test/dangling-hardlink:latest".
func PushDanglingHardlinkImage(ctx context.Context, ref string) error {
	img, err := CreateDanglingHardlinkImage()
	if err != nil {
		return err
	}
	tag, err := name.NewTag(ref, name.Insecure)
	if err != nil {
		return fmt.Errorf("parse tag %q: %w", ref, err)
	}
	return remote.Write(tag, img, remote.WithContext(ctx))
}

// layerHasDanglingHardlink scans an (uncompressed) layer tar and reports
// whether it contains a hardlink to linkTarget while linkTarget itself is not
// a regular-file member — i.e. a dangling hardlink.  Exposed for tests.
func layerHasDanglingHardlink(layer v1.Layer, linkTarget string) (bool, error) {
	rc, err := layer.Uncompressed()
	if err != nil {
		return false, err
	}
	defer rc.Close()

	tr := tar.NewReader(rc)
	sawLink := false
	targetPresent := false
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return false, err
		}
		if hdr.Typeflag == tar.TypeLink && hdr.Linkname == linkTarget {
			sawLink = true
		}
		if hdr.Name == linkTarget {
			targetPresent = true
		}
	}
	return sawLink && !targetPresent, nil
}
