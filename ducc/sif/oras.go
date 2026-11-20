package sif

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

const (
	sifLayerMediaType  = "application/vnd.sylabs.sif.layer.v1.sif"
	ociManifestType    = "application/vnd.oci.image.manifest.v1+json"
	dockerManifestType = "application/vnd.docker.distribution.manifest.v2+json"
)

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

// registryAuth implements the registry token flow: a 401 carries a
// WWW-Authenticate challenge, answered with a bearer token from the realm
// (or basic credentials). Tokens expire during long ingests, so refresh is
// called again on any later 401.
type registryAuth struct {
	client     *http.Client
	user, pass string

	mu     sync.Mutex
	header string
	last   time.Time
}

func (a *registryAuth) apply(req *http.Request) {
	a.mu.Lock()
	h := a.header
	a.mu.Unlock()
	if h != "" {
		req.Header.Set("Authorization", h)
	}
}

// refresh reports whether a new Authorization header was obtained.
func (a *registryAuth) refresh(ctx context.Context, resp *http.Response) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	// Concurrent chunk fetches all see the 401; one refresh serves them.
	if time.Since(a.last) < time.Second && a.header != "" {
		return true
	}
	scheme, params := parseChallenge(resp.Header.Get("WWW-Authenticate"))
	switch strings.ToLower(scheme) {
	case "basic":
		if a.user == "" {
			return false
		}
		a.header = "Basic " + base64.StdEncoding.EncodeToString([]byte(a.user+":"+a.pass))
	case "bearer":
		tok, err := a.fetchToken(ctx, params)
		if err != nil {
			return false
		}
		a.header = "Bearer " + tok
	default:
		return false
	}
	a.last = time.Now()
	return true
}

func (a *registryAuth) fetchToken(ctx context.Context, params map[string]string) (string, error) {
	realm := params["realm"]
	if realm == "" {
		return "", fmt.Errorf("bearer challenge without realm")
	}
	u, err := url.Parse(realm)
	if err != nil {
		return "", err
	}
	q := u.Query()
	for _, k := range []string{"service", "scope"} {
		if v := params[k]; v != "" {
			q.Set(k, v)
		}
	}
	u.RawQuery = q.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return "", err
	}
	if a.user != "" {
		req.SetBasicAuth(a.user, a.pass)
	}
	resp, err := a.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("token request: HTTP %s", resp.Status)
	}
	var body struct {
		Token       string `json:"token"`
		AccessToken string `json:"access_token"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&body); err != nil {
		return "", err
	}
	if body.Token == "" {
		body.Token = body.AccessToken
	}
	if body.Token == "" {
		return "", fmt.Errorf("token response without token")
	}
	return body.Token, nil
}

// parseChallenge parses `Bearer realm="...",service="...",scope="..."`.
func parseChallenge(h string) (string, map[string]string) {
	scheme, rest, _ := strings.Cut(strings.TrimSpace(h), " ")
	params := map[string]string{}
	for rest != "" {
		rest = strings.TrimLeft(rest, " ,")
		k, v, ok := strings.Cut(rest, "=")
		if !ok {
			break
		}
		k = strings.ToLower(strings.TrimSpace(k))
		if strings.HasPrefix(v, `"`) {
			end := strings.Index(v[1:], `"`)
			if end < 0 {
				params[k] = v[1:]
				break
			}
			params[k], rest = v[1:end+1], v[end+2:]
		} else {
			val, r, _ := strings.Cut(v, ",")
			params[k], rest = strings.TrimSpace(val), r
		}
	}
	return scheme, params
}

type ociManifest struct {
	MediaType string `json:"mediaType"`
	Layers    []struct {
		MediaType string `json:"mediaType"`
		Digest    string `json:"digest"`
		Size      int64  `json:"size"`
	} `json:"layers"`
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
	if opts.PlainHTTP {
		scheme = "http"
	}
	base := scheme + "://" + r.Registry + "/v2/" + r.Repository
	client := &http.Client{Timeout: 5 * time.Minute}
	if opts.Client != nil {
		*client = *opts.Client
	}
	// Blob GETs redirect to object storage. Go keeps Authorization when only
	// the port differs, so drop it on any change of host:port.
	client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		if len(via) >= 10 {
			return fmt.Errorf("stopped after 10 redirects")
		}
		if req.URL.Host != via[0].URL.Host {
			req.Header.Del("Authorization")
		}
		return nil
	}
	opts.Client = client
	auth := &registryAuth{client: client}
	if opts.Credentials != nil {
		auth.user, auth.pass, _ = opts.Credentials(scheme + "://" + r.Registry)
	}

	reference := r.Tag
	if r.Digest != "" {
		reference = "sha256:" + r.Digest
	}
	var m ociManifest
	for attempt := 0; ; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/manifests/"+reference, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Accept", ociManifestType+", "+dockerManifestType)
		auth.apply(req)
		resp, err := opts.Client.Do(req)
		if err != nil {
			return nil, err
		}
		if resp.StatusCode == http.StatusUnauthorized && attempt == 0 && auth.refresh(ctx, resp) {
			resp.Body.Close()
			continue
		}
		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			return nil, fmt.Errorf("%s: fetching manifest %s: HTTP %s", ref, reference, resp.Status)
		}
		err = json.NewDecoder(io.LimitReader(resp.Body, 4<<20)).Decode(&m)
		resp.Body.Close()
		if err != nil {
			return nil, fmt.Errorf("%s: invalid manifest: %w", ref, err)
		}
		break
	}

	var layer *struct {
		MediaType string `json:"mediaType"`
		Digest    string `json:"digest"`
		Size      int64  `json:"size"`
	}
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
	digest, err := normalizeDigest(layer.Digest)
	if err != nil {
		return nil, fmt.Errorf("%s: layer digest: %w", ref, err)
	}
	src, err := newHTTPSource(ctx, base+"/blobs/sha256:"+digest, opts, auth, digest)
	if err != nil {
		return nil, err
	}
	if layer.Size > 0 && src.Size() != layer.Size {
		src.Close()
		return nil, fmt.Errorf("%s: blob has %d bytes, manifest says %d", ref, src.Size(), layer.Size)
	}
	return src, nil
}
