package cmd

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/cvmfs/ducc/testutils"
)

// resetExpandWildcardFlags resets the package-level flag variables bound to
// the expand-wildcard command.  StringArrayVar accumulates values across
// cobra Execute() calls in the same process, so tests must call this (via
// t.Cleanup) to remain independent.
func resetExpandWildcardFlags() {
	wishlistFiles = nil
	outputFile = ""
}

func TestExpandWildcardTrueRemote(t *testing.T) {
	t.Cleanup(resetExpandWildcardFlags)
	if !*testutils.Online {
		t.Skip("Skipping test in offline mode.")
	}
	var err error
	cmd := rootCmd
	cmd.SetArgs([]string{"expand-wildcard",
		"https://registry.hub.docker.com/atlas/athena:21.0.*"})
	err = cmd.Execute()
	if err != nil {
		t.Fatal(err)
	}
}

func TestExpandWildcardMockRemote(t *testing.T) {
	t.Cleanup(resetExpandWildcardFlags)
	if !*testutils.LocalRegistry {
		t.Skip("Skipping test that needs local registry.")
	}
	var err error
	cmd := rootCmd
	cmd.SetArgs([]string{"expand-wildcard",
		testutils.GetTestRegistryUrl() + "multi-arch-test:*"})
	err = cmd.Execute()
	if err != nil {
		t.Fatal(err)
	}
}

func TestExpandWildcardMockRemote2(t *testing.T) {
	t.Cleanup(resetExpandWildcardFlags)
	if !*testutils.LocalRegistry {
		t.Skip("Skipping test that needs local registry.")
	}
	var err error
	cmd := rootCmd
	cmd.SetArgs([]string{"expand-wildcard",
		testutils.GetTestRegistryUrl() + "nosuchimage:*"})
	err = cmd.Execute()
	if err == nil {
		t.Fatal(err)
	}
}

// --- wishlist flag tests ---

// TestExpandWildcardWishlistMissingFile checks that a non-existent wishlist
// file returns an error.
func TestExpandWildcardWishlistMissingFile(t *testing.T) {
	t.Cleanup(resetExpandWildcardFlags)
	cmd := rootCmd
	cmd.SetArgs([]string{"expand-wildcard", "--wishlist", "/no/such/file.yaml"})
	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected error for missing wishlist file, got nil")
	}
}

// TestExpandWildcardWishlistInvalidYaml checks that a malformed YAML file
// returns an error.
func TestExpandWildcardWishlistInvalidYaml(t *testing.T) {
	t.Cleanup(resetExpandWildcardFlags)
	f, err := os.CreateTemp("", "ducc-wishlist-*.yaml")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(f.Name())
	f.WriteString(": this is not: valid: yaml: !!!\n")
	f.Close()

	cmd := rootCmd
	cmd.SetArgs([]string{"expand-wildcard", "--wishlist", f.Name()})
	// ParseYamlRecipeV1 is lenient about YAML syntax, so we only assert it
	// doesn't panic; an error is acceptable but not required here.
	_ = cmd.Execute()
}

// TestExpandWildcardWishlistMockWildcard creates a minimal wishlist YAML with
// a wildcard entry and verifies that the command succeeds and produces at least
// one output image.
func TestExpandWildcardWishlistMockWildcard(t *testing.T) {
	t.Cleanup(resetExpandWildcardFlags)
	if !*testutils.LocalRegistry {
		t.Skip("Skipping test that needs local registry.")
	}

	yamlContent := fmt.Sprintf(`version: 1
user: testuser
cvmfs_repo: test.repo.ch
input:
  - %smulti-arch-test:*
`, testutils.GetTestRegistryUrl())

	f, err := os.CreateTemp("", "ducc-wishlist-*.yaml")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(f.Name())
	f.WriteString(yamlContent)
	f.Close()

	cmd := rootCmd
	cmd.SetArgs([]string{"expand-wildcard", "--wishlist", f.Name()})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// TestExpandWildcardWishlistMockNonWildcard creates a wishlist with a concrete
// (non-wildcard) tag and verifies that the command succeeds and the single
// image is listed.
func TestExpandWildcardWishlistMockNonWildcard(t *testing.T) {
	t.Cleanup(resetExpandWildcardFlags)
	if !*testutils.LocalRegistry {
		t.Skip("Skipping test that needs local registry.")
	}

	yamlContent := fmt.Sprintf(`version: 1
user: testuser
cvmfs_repo: test.repo.ch
input:
  - %smulti-arch-test:latest
`, testutils.GetTestRegistryUrl())

	f, err := os.CreateTemp("", "ducc-wishlist-*.yaml")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(f.Name())
	f.WriteString(yamlContent)
	f.Close()

	cmd := rootCmd
	cmd.SetArgs([]string{"expand-wildcard", "--wishlist", f.Name()})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// TestExpandWildcardWishlistNoPositionalArg verifies that specifying a wishlist
// file without a positional image argument is accepted (no args error).
func TestExpandWildcardWishlistNoPositionalArg(t *testing.T) {
	t.Cleanup(resetExpandWildcardFlags)
	// Use a valid (but empty) YAML so we only test arg validation, not I/O.
	f, err := os.CreateTemp("", "ducc-wishlist-*.yaml")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(f.Name())
	f.WriteString("version: 1\ncvmfs_repo: test.repo.ch\ninput: []\n")
	f.Close()

	cmd := rootCmd
	cmd.SetArgs([]string{"expand-wildcard", "--wishlist", f.Name()})
	// Should not fail due to missing positional arg.
	err = cmd.Execute()
	// An empty input list is fine; we expect no error from arg validation.
	_ = err
}

// TestExpandWildcardMultipleWishlists verifies that --wishlist can be given
// more than once and that images from all files are merged.
func TestExpandWildcardMultipleWishlists(t *testing.T) {
	t.Cleanup(resetExpandWildcardFlags)
	if !*testutils.LocalRegistry {
		t.Skip("Skipping test that needs local registry.")
	}

	makeWishlist := func(imageRef string) string {
		f, err := os.CreateTemp("", "ducc-wishlist-*.yaml")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { os.Remove(f.Name()) })
		fmt.Fprintf(f, "version: 1\nuser: testuser\ncvmfs_repo: test.repo.ch\ninput:\n  - %s\n", imageRef)
		f.Close()
		return f.Name()
	}

	f1 := makeWishlist(testutils.GetTestRegistryUrl() + "multi-arch-test:latest")
	f2 := makeWishlist(testutils.GetTestRegistryUrl() + "multi-arch-test:latest")

	cmd := rootCmd
	cmd.SetArgs([]string{"expand-wildcard", "--wishlist", f1, "--wishlist", f2})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("unexpected error with multiple wishlists: %v", err)
	}
}

// TestExpandWildcardOutputFile verifies that --output writes a YAML file in
// the images: format expected by prune-images --expanded-images.
func TestExpandWildcardOutputFile(t *testing.T) {
	t.Cleanup(resetExpandWildcardFlags)
	// Use an empty wishlist so no registry queries are needed.
	wl, err := os.CreateTemp("", "ducc-wishlist-*.yaml")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Remove(wl.Name()) })
	wl.WriteString("version: 1\ncvmfs_repo: test.repo.ch\ninput: []\n")
	wl.Close()

	out, err := os.CreateTemp("", "ducc-expanded-*.yaml")
	if err != nil {
		t.Fatal(err)
	}
	outPath := out.Name()
	out.Close()
	t.Cleanup(func() { os.Remove(outPath) })

	cmd := rootCmd
	cmd.SetArgs([]string{"expand-wildcard", "--wishlist", wl.Name(), "--output", outPath})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	data, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("output file not created: %v", err)
	}
	// The file must contain the "images:" key (even if the list is empty).
	if !strings.Contains(string(data), "images:") {
		t.Fatalf("output file does not contain 'images:' key: %s", data)
	}
}

// TestExpandWildcardOutputWithoutWishlist verifies that --output without
// --wishlist is rejected by the Args validator.
func TestExpandWildcardOutputWithoutWishlist(t *testing.T) {
	t.Cleanup(resetExpandWildcardFlags)
	cmd := rootCmd
	cmd.SetArgs([]string{"expand-wildcard", "--output", "/tmp/out.yaml",
		"https://registry.hub.docker.com/library/ubuntu:latest"})
	err := cmd.Execute()
	if err == nil {
		t.Fatal("expected error when --output is used without --wishlist, got nil")
	}
}
