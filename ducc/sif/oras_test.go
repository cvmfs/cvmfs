package sif

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

func TestParseOrasRef(t *testing.T) {
	d := strings.Repeat("ab", 32)
	cases := map[string]OrasRef{
		"oras://ghcr.io/org/img:v1":                  {"ghcr.io", "org/img", "v1", ""},
		"oras://localhost:5000/a/b/c":                {"localhost:5000", "a/b/c", "latest", ""},
		"oras://reg.example/img@sha256:" + d:         {"reg.example", "img", "", d},
		"oras://reg.example:443/x/img:t@sha256:" + d: {"reg.example:443", "x/img", "t", d},
	}
	for in, want := range cases {
		got, err := ParseOrasRef(in)
		if err != nil || got != want {
			t.Errorf("%s: got %+v, %v", in, got, err)
		}
	}
	for _, bad := range []string{"oras://host", "oras://host/", "https://x/y", "oras://h/../x:1", "oras://h/x@sha256:12"} {
		if _, err := ParseOrasRef(bad); err == nil {
			t.Errorf("%s accepted", bad)
		}
	}
	if n := DefaultName("oras://ghcr.io/org/img:v1"); n != "ghcr.io/org/img:v1" {
		t.Errorf("default name %q", n)
	}
	if n := DefaultName("oras://r/i@sha256:" + d); n != "r/i@sha256:"+d {
		t.Errorf("default name %q", n)
	}
}

// fakeRegistry serves one SIF as an oras artifact behind bearer-token auth.
// Each token stops working after tokenUses requests; blobs redirect to
// storage.
type fakeRegistry struct {
	srv, storage *httptest.Server
	data         []byte
	digest       string
	tokenUses    int32
	tokens       int32 // tokens issued

	mu        sync.Mutex
	tokenLeft map[string]int32
}

func newFakeRegistry(t *testing.T, data []byte, tokenUses int32) *fakeRegistry {
	sum := sha256.Sum256(data)
	r := &fakeRegistry{data: data, digest: hex.EncodeToString(sum[:]), tokenUses: tokenUses,
		tokenLeft: map[string]int32{}}
	r.storage = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Header.Get("Authorization") != "" {
			t.Errorf("registry credentials leaked to storage host")
		}
		w.Header().Set("ETag", `"`+r.digest+`"`)
		http.ServeContent(w, req, "blob", time.Unix(1, 0), bytes.NewReader(r.data))
	}))
	r.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path == "/token" {
			if u, p, ok := req.BasicAuth(); !ok || u != "alice" || p != "secret" {
				http.Error(w, "bad login", http.StatusUnauthorized)
				return
			}
			if req.URL.Query().Get("scope") != "repository:test/img:pull" {
				t.Errorf("unexpected scope %q", req.URL.Query().Get("scope"))
			}
			tok := fmt.Sprintf("tok%d", atomic.AddInt32(&r.tokens, 1))
			r.mu.Lock()
			r.tokenLeft[tok] = r.tokenUses
			r.mu.Unlock()
			json.NewEncoder(w).Encode(map[string]string{"token": tok})
			return
		}
		tok, _ := strings.CutPrefix(req.Header.Get("Authorization"), "Bearer ")
		r.mu.Lock()
		left, ok := r.tokenLeft[tok]
		r.tokenLeft[tok] = left - 1
		r.mu.Unlock()
		if !ok || left <= 0 {
			w.Header().Set("WWW-Authenticate", fmt.Sprintf(
				`Bearer realm="%s/token",service="fake",scope="repository:test/img:pull"`, r.srv.URL))
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		switch req.URL.Path {
		case "/v2/test/img/manifests/v1", "/v2/test/img/manifests/sha256:" + r.digest:
			w.Header().Set("Content-Type", ocispec.MediaTypeImageManifest)
			fmt.Fprintf(w, `{"schemaVersion":2,"mediaType":%q,
				"config":{"mediaType":"application/vnd.sylabs.sif.config.v1+json","digest":"sha256:%s","size":2},
				"layers":[{"mediaType":%q,"digest":"sha256:%s","size":%d}]}`,
				ocispec.MediaTypeImageManifest, strings.Repeat("0", 64), sifLayerMediaType, r.digest, len(r.data))
		case "/v2/test/img/blobs/sha256:" + r.digest:
			http.Redirect(w, req, r.storage.URL+"/blob", http.StatusTemporaryRedirect)
		default:
			http.NotFound(w, req)
		}
	}))
	t.Cleanup(func() { r.srv.Close(); r.storage.Close() })
	return r
}

func (r *fakeRegistry) host() string { return strings.TrimPrefix(r.srv.URL, "http://") }

func orasOpts() SourceOptions {
	o := DefaultSourceOptions()
	o.PlainHTTP = true
	o.ChunkSize = 1 << 20
	o.Credentials = func(string) (string, string, bool) { return "alice", "secret", true }
	return o
}

func TestOrasSource(t *testing.T) {
	path := bigImage(t)
	data, _ := os.ReadFile(path)
	// tokens expire every 5 requests, so a full read must refresh them
	reg := newFakeRegistry(t, data, 5)

	fsrc, _ := OpenSource(context.Background(), path, DefaultSourceOptions())
	want, err := streamFrom(t, fsrc, 4)
	if err != nil {
		t.Fatal(err)
	}
	src, err := OpenSource(context.Background(), "oras://"+reg.host()+"/test/img:v1", orasOpts())
	if err != nil {
		t.Fatal(err)
	}
	if src.KnownDigest() != reg.digest {
		t.Fatalf("digest %q, want %q", src.KnownDigest(), reg.digest)
	}
	got, err := streamFrom(t, src, 4)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("tar via oras differs from tar from file")
	}
	if atomic.LoadInt32(&reg.tokens) < 2 {
		t.Errorf("expected token refreshes, got %d tokens", reg.tokens)
	}

	if _, err := OpenSource(context.Background(), "oras://"+reg.host()+"/test/img@sha256:"+reg.digest, orasOpts()); err != nil {
		t.Errorf("digest reference: %v", err)
	}
	bad := orasOpts()
	bad.Credentials = func(string) (string, string, bool) { return "alice", "wrong", true }
	if _, err := OpenSource(context.Background(), "oras://"+reg.host()+"/test/img:v1", bad); err == nil {
		t.Error("wrong credentials accepted")
	}
}

func TestSpoolFallback(t *testing.T) {
	path := bigImage(t)
	data, _ := os.ReadFile(path)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write(data) // ignores Range
	}))
	defer srv.Close()

	spool := t.TempDir()
	opts := DefaultSourceOptions()
	opts.SpoolDir = spool
	src, err := OpenSource(context.Background(), srv.URL, opts)
	if err != nil {
		t.Fatal(err)
	}
	fsrc, _ := OpenSource(context.Background(), path, DefaultSourceOptions())
	want, _ := streamFrom(t, fsrc, 2)
	got, err := streamFrom(t, src, 2)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("spooled stream differs: %v", err)
	}
	if files, _ := filepath.Glob(filepath.Join(spool, "*")); len(files) != 1 {
		t.Errorf("want one spool file, got %v", files)
	}
	src.Close()
	if files, _ := filepath.Glob(filepath.Join(spool, "*")); len(files) != 0 {
		t.Errorf("spool file left behind: %v", files)
	}

	// a known digest that does not match the download is rejected
	s := &httpSource{ctx: context.Background(), url: srv.URL, client: http.DefaultClient,
		attempts: 1, digest: strings.Repeat("0", 64), logger: opts.Logger}
	if s.logger == nil {
		s.logger = defaultLogger()
	}
	if _, err := s.spool(spool); err == nil || !strings.Contains(err.Error(), "expected") {
		t.Errorf("digest mismatch not detected: %v", err)
	}
	if files, _ := filepath.Glob(filepath.Join(spool, "*")); len(files) != 0 {
		t.Errorf("spool file left after failed download: %v", files)
	}
}
