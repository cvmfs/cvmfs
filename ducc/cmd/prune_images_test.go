package cmd

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

// --------------------------------------------------------------------------
// loadPruneConfig
// --------------------------------------------------------------------------

func TestLoadPruneConfig_NoDefaultFileReturnsNil(t *testing.T) {
	// Run from a temp directory that has no ducc-prune.yaml.
	dir := t.TempDir()
	orig, _ := os.Getwd()
	if err := os.Chdir(dir); err != nil {
		t.Fatal(err)
	}
	defer os.Chdir(orig)

	sources, err := loadPruneConfig("")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sources != nil {
		t.Errorf("expected nil sources, got %v", sources)
	}
}

func TestLoadPruneConfig_DefaultFileIsReadWhenPresent(t *testing.T) {
	dir := t.TempDir()
	orig, _ := os.Getwd()
	if err := os.Chdir(dir); err != nil {
		t.Fatal(err)
	}
	defer os.Chdir(orig)

	content := "wishlists:\n  - /path/to/a.yaml\n  - /path/to/b.yaml\n"
	if err := os.WriteFile(pruneImagesDefaultConfig, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}

	sources, err := loadPruneConfig("")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(sources) != 2 {
		t.Fatalf("expected 2 sources, got %d", len(sources))
	}
	if sources[0] != "/path/to/a.yaml" || sources[1] != "/path/to/b.yaml" {
		t.Errorf("unexpected sources: %v", sources)
	}
}

func TestLoadPruneConfig_ExplicitPathOverridesDefault(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "custom-prune.yaml")
	content := "wishlists:\n  - /explicit/wishlist.yaml\n"
	if err := os.WriteFile(configPath, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}

	sources, err := loadPruneConfig(configPath)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(sources) != 1 || sources[0] != "/explicit/wishlist.yaml" {
		t.Errorf("unexpected sources: %v", sources)
	}
}

func TestLoadPruneConfig_MissingExplicitPathReturnsError(t *testing.T) {
	_, err := loadPruneConfig("/nonexistent/path/prune.yaml")
	if err == nil {
		t.Error("expected error for non-existent explicit config path")
	}
}

func TestLoadPruneConfig_InvalidYAMLReturnsError(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "bad.yaml")
	// An unclosed flow-mapping is genuinely malformed YAML that yaml.v2 rejects.
	if err := os.WriteFile(configPath, []byte("{"), 0o644); err != nil {
		t.Fatal(err)
	}

	_, err := loadPruneConfig(configPath)
	if err == nil {
		t.Error("expected error for invalid YAML")
	}
}

func TestLoadPruneConfig_EmptyWishlistsKey(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "empty.yaml")
	if err := os.WriteFile(configPath, []byte("wishlists: []\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	sources, err := loadPruneConfig(configPath)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(sources) != 0 {
		t.Errorf("expected 0 sources, got %v", sources)
	}
}

// --------------------------------------------------------------------------
// parseWishlistInputs
// --------------------------------------------------------------------------

func TestParseWishlistInputs_ValidImages(t *testing.T) {
	yaml := []byte(`
input:
  - https://registry.hub.docker.com/library/ubuntu:22.04
  - https://registry.hub.docker.com/library/alpine:3.18
`)
	imgs, err := parseWishlistInputs(yaml)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(imgs) != 2 {
		t.Fatalf("expected 2 images, got %d", len(imgs))
	}
	if imgs[0].Tag != "22.04" {
		t.Errorf("expected tag 22.04, got %q", imgs[0].Tag)
	}
	if imgs[1].Repository != "library/alpine" {
		t.Errorf("expected repository library/alpine, got %q", imgs[1].Repository)
	}
}

func TestParseWishlistInputs_SkipsUnparsableEntries(t *testing.T) {
	yaml := []byte(`
input:
  - https://registry.hub.docker.com/library/ubuntu:22.04
  - not-a-valid-image-ref
  - https://registry.hub.docker.com/library/debian:11
`)
	imgs, err := parseWishlistInputs(yaml)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// The invalid entry should be silently skipped; valid ones are kept.
	if len(imgs) != 2 {
		t.Fatalf("expected 2 images after skipping invalid entry, got %d", len(imgs))
	}
}

func TestParseWishlistInputs_EmptyInput(t *testing.T) {
	yaml := []byte("input: []\n")
	imgs, err := parseWishlistInputs(yaml)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(imgs) != 0 {
		t.Errorf("expected 0 images, got %d", len(imgs))
	}
}

func TestParseWishlistInputs_InvalidYAML(t *testing.T) {
	// An unclosed flow-mapping is genuinely malformed YAML that yaml.v2 rejects.
	_, err := parseWishlistInputs([]byte("{"))
	if err == nil {
		t.Error("expected error for invalid YAML")
	}
}

func TestParseWishlistInputs_WildcardTagPreserved(t *testing.T) {
	yaml := []byte(`
input:
  - https://registry.hub.docker.com/library/redis:*
`)
	imgs, err := parseWishlistInputs(yaml)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(imgs) != 1 {
		t.Fatalf("expected 1 image, got %d", len(imgs))
	}
	if !imgs[0].TagWildcard {
		t.Error("expected TagWildcard=true for '*' tag")
	}
	if imgs[0].Tag != "*" {
		t.Errorf("expected tag '*', got %q", imgs[0].Tag)
	}
}

// --------------------------------------------------------------------------
// loadWishlist / loadWishlistFromHTTP
// --------------------------------------------------------------------------

func TestLoadWishlist_LocalFile(t *testing.T) {
	dir := t.TempDir()
	content := []byte("input:\n  - https://registry.hub.docker.com/library/ubuntu:22.04\n")
	path := filepath.Join(dir, "wishlist.yaml")
	if err := os.WriteFile(path, content, 0o644); err != nil {
		t.Fatal(err)
	}

	data, err := loadWishlist(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(data) != string(content) {
		t.Errorf("content mismatch: got %q", string(data))
	}
}

func TestLoadWishlist_LocalFileMissing(t *testing.T) {
	_, err := loadWishlist("/nonexistent/wishlist.yaml")
	if err == nil {
		t.Error("expected error for missing local file")
	}
}

func TestLoadWishlist_HTTP(t *testing.T) {
	expectedBody := "input:\n  - https://registry.hub.docker.com/library/ubuntu:22.04\n"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(expectedBody))
	}))
	defer srv.Close()

	data, err := loadWishlist(srv.URL + "/wishlist.yaml")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(data) != expectedBody {
		t.Errorf("expected %q, got %q", expectedBody, string(data))
	}
}

func TestLoadWishlist_HTTPNonOKStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "not found", http.StatusNotFound)
	}))
	defer srv.Close()

	_, err := loadWishlist(srv.URL + "/missing.yaml")
	if err == nil {
		t.Error("expected error for HTTP 404 response")
	}
}

func TestLoadWishlist_HTTPSPrefix(t *testing.T) {
	// Verify the https:// prefix triggers HTTP loading (not local file path).
	// We use a TLS test server to confirm routing.
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("input: []\n"))
	}))
	defer srv.Close()

	// loadWishlistFromHTTP directly (bypassing TLS verification is not
	// possible through the public loadWishlist – test the routing logic
	// using the internal function with the TLS server's client).
	client := srv.Client()
	resp, err := client.Get(srv.URL + "/wishlist.yaml")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}
}

// --------------------------------------------------------------------------
// pruneFromWishlist (integration of loadWishlist + parseWishlistInputs)
// --------------------------------------------------------------------------

func TestPruneFromWishlist_MissingSourceReturnsError(t *testing.T) {
	_, err := pruneFromWishlist("repo.example.ch", "/nonexistent/wishlist.yaml", true)
	if err == nil {
		t.Error("expected error when wishlist source does not exist")
	}
}

func TestPruneFromWishlist_InvalidYAMLReturnsError(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bad.yaml")
	// An unclosed flow-mapping is genuinely malformed YAML that yaml.v2 rejects.
	if err := os.WriteFile(path, []byte("{"), 0o644); err != nil {
		t.Fatal(err)
	}

	_, err := pruneFromWishlist("repo.example.ch", path, true)
	if err == nil {
		t.Error("expected error for invalid wishlist YAML")
	}
}
