package sif

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/v1/remote/transport"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

const sifLayerMediaType = "application/vnd.sylabs.sif.layer.v1.sif"

// OrasRef is a parsed oras://<registry>/<repository>[:<tag>][@sha256:<hex>].
type OrasRef struct {
	Registry   string
	Repository string
	Tag        string
	Digest     string // hex, without the sha256: prefix
}

func ParseOrasRef(ref string) (OrasRef, error) {
	var r OrasRef
	rest, ok := strings.CutPrefix(ref, "oras://")
	if !ok {
		return r, fmt.Errorf("%q is not an oras:// reference", ref)
	}
	if i := strings.Index(rest, "@"); i >= 0 {
		d, err := normalizeDigest(rest[i+1:])
		if err != nil {
			return r, err
		}
		r.Digest, rest = d, rest[:i]
	}
	slash := strings.Index(rest, "/")
	if slash <= 0 || slash == len(rest)-1 {
		return r, fmt.Errorf("oras reference %q needs a registry and a repository", ref)
	}
	r.Registry, r.Repository = rest[:slash], rest[slash+1:]
	if i := strings.LastIndex(r.Repository, ":"); i > strings.LastIndex(r.Repository, "/") {
		r.Tag, r.Repository = r.Repository[i+1:], r.Repository[:i]
	}
	if r.Tag == "" && r.Digest == "" {
		r.Tag = "latest"
	}
	if r.Repository == "" || strings.Contains(r.Repository, "..") {
		return r, fmt.Errorf("invalid repository in oras reference %q", ref)
	}
	return r, nil
}

// PublicName is the default name below sif/: registry/repository:tag, or
// registry/repository@sha256:hex for digest-only references.
func (r OrasRef) PublicName() string {
	if r.Tag != "" {
		return r.Registry + "/" + r.Repository + ":" + r.Tag
	}
	return r.Registry + "/" + r.Repository + "@sha256:" + r.Digest
}

// DefaultName derives the public name from the source when it implies one.
func DefaultName(source string) string {
	if r, err := ParseOrasRef(source); err == nil {
		return r.PublicName()
	}
	return ""
}

// openOras resolves the reference to the SIF layer blob and reads it with
// range requests like any HTTP source; the blob digest becomes the image
// ID.
func openOras(ctx context.Context, ref string, opts SourceOptions) (Source, error) {
	r, err := ParseOrasRef(ref)
	if err != nil {
		return nil, err
	}
	scheme := "https"
	var nameOpts []name.Option
	if opts.PlainHTTP {
		scheme = "http"
		nameOpts = append(nameOpts, name.Insecure)
	}
	// Built from parts: name.ParseReference would send a registry without
	// a dot or port to Docker Hub.
	reg, err := name.NewRegistry(r.Registry, nameOpts...)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", ref, err)
	}
	repo := reg.Repo(r.Repository)
	auth := authn.Anonymous
	if opts.Credentials != nil {
		if user, pass, ok := opts.Credentials(scheme + "://" + r.Registry); ok {
			auth = authn.FromConfig(authn.AuthConfig{Username: user, Password: pass})
		}
	}

	client := &http.Client{Timeout: 5 * time.Minute}
	if opts.Client != nil {
		*client = *opts.Client
	}
	inner := client.Transport
	if inner == nil {
		inner = http.DefaultTransport
	}
	// Refreshes expired tokens on 401 and sends credentials only to the
	// registry, not to the storage that blob GETs redirect to.
	client.Transport, err = transport.NewWithContext(ctx, reg, auth, inner,
		[]string{repo.Scope(transport.PullScope)})
	if err != nil {
		return nil, fmt.Errorf("%s: %w", ref, err)
	}
	opts.Client = client

	base := scheme + "://" + r.Registry + "/v2/" + r.Repository
	reference := r.Tag
	if r.Digest != "" {
		reference = "sha256:" + r.Digest
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/manifests/"+reference, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", ocispec.MediaTypeImageManifest+", "+mediaTypeDockerManifest)
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%s: fetching manifest %s: HTTP %s", ref, reference, resp.Status)
	}
	var m ocispec.Manifest
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxOCIJSON)).Decode(&m); err != nil {
		return nil, fmt.Errorf("%s: invalid manifest: %w", ref, err)
	}

	var layer *ocispec.Descriptor
	var types []string
	for i := range m.Layers {
		types = append(types, m.Layers[i].MediaType)
		if m.Layers[i].MediaType == sifLayerMediaType {
			if layer != nil {
				return nil, fmt.Errorf("%s: manifest has more than one SIF layer", ref)
			}
			layer = &m.Layers[i]
		}
	}
	if layer == nil {
		return nil, fmt.Errorf("%s: no %s layer in manifest (layers: %v)", ref, sifLayerMediaType, types)
	}
	digest, err := normalizeDigest(string(layer.Digest))
	if err != nil {
		return nil, fmt.Errorf("%s: layer digest: %w", ref, err)
	}
	src, err := newHTTPSource(ctx, base+"/blobs/sha256:"+digest, opts, digest)
	if err != nil {
		return nil, err
	}
	if layer.Size > 0 && src.Size() != layer.Size {
		src.Close()
		return nil, fmt.Errorf("%s: blob has %d bytes, manifest says %d", ref, src.Size(), layer.Size)
	}
	return src, nil
}
