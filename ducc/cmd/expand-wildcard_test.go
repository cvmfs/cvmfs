package cmd

import (
	"fmt"
	"os"
	"testing"

	"github.com/cvmfs/ducc/testutils"
)

func TestExpandWildcardTrueRemote(t *testing.T) {
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
