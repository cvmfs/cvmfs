package cvmfs

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

// These tests exercise the ingest-based write helpers used for mountless
// gateway publishing.  They run against the mock cvmfs_server (set up in
// TestMain), whose ingest handler extracts the streamed tar into the mock
// repository — the same path a real mountless publisher would take.

func withMountlessPublishing(t *testing.T) {
	t.Helper()
	SetMountlessPublishing(true)
	t.Cleanup(func() { SetMountlessPublishing(false) })
}

func TestMountlessPublishFile(t *testing.T) {
	withMountlessPublishing(t)
	mockrepo := filepath.Clean("/" + os.Getenv("CVMFS_TEST_REPO"))

	f, err := os.CreateTemp("", "MountlessPublishFile")
	if err != nil {
		t.Fatal(err)
	}
	content := []byte("mountless-file-content")
	if _, err := f.Write(content); err != nil {
		t.Fatal(err)
	}
	f.Close()

	if err := PublishToCVMFS(".."+mockrepo, "sub/dir/published.txt", f.Name()); err != nil {
		t.Fatalf("mountless PublishToCVMFS failed: %v", err)
	}

	readback, err := os.ReadFile(filepath.Join(mockrepo, "sub", "dir", "published.txt"))
	if err != nil {
		t.Fatalf("published file not found: %v", err)
	}
	if !bytes.Equal(readback, content) {
		t.Fatalf("published content differs: got %q", readback)
	}
}

func TestMountlessPublishFileAtRoot(t *testing.T) {
	withMountlessPublishing(t)
	mockrepo := filepath.Clean("/" + os.Getenv("CVMFS_TEST_REPO"))

	f, err := os.CreateTemp("", "MountlessRootFile")
	if err != nil {
		t.Fatal(err)
	}
	content := []byte("root-content")
	if _, err := f.Write(content); err != nil {
		t.Fatal(err)
	}
	f.Close()

	// A repository-root path yields an empty ingest base_dir; make sure that
	// is handled (no -B passed to the real server; root extraction in the mock).
	if err := PublishToCVMFS(".."+mockrepo, "root_published.txt", f.Name()); err != nil {
		t.Fatalf("mountless PublishToCVMFS at root failed: %v", err)
	}

	readback, err := os.ReadFile(filepath.Join(mockrepo, "root_published.txt"))
	if err != nil {
		t.Fatalf("root-published file not found: %v", err)
	}
	if !bytes.Equal(readback, content) {
		t.Fatalf("root-published content differs: got %q", readback)
	}
}

func TestMountlessCreateSymlink(t *testing.T) {
	withMountlessPublishing(t)
	mockrepo := filepath.Clean("/" + os.Getenv("CVMFS_TEST_REPO"))

	// The mountless path does not stat the target (no mount), so the target
	// need not exist; the stored link is pure path arithmetic.
	if err := CreateSymlinkIntoCVMFS(".."+mockrepo, "images/myimage", ".flat/deadbeef"); err != nil {
		t.Fatalf("mountless CreateSymlinkIntoCVMFS failed: %v", err)
	}

	linkPath := filepath.Join(mockrepo, "images", "myimage")
	target, err := os.Readlink(linkPath)
	if err != nil {
		t.Fatalf("symlink not created at %s: %v", linkPath, err)
	}
	// Rel(/cvmfs/<r>/images/myimage, /cvmfs/<r>/.flat/deadbeef) -> ../../.flat/deadbeef
	// with the first chunk (the link's own dir) dropped -> ../.flat/deadbeef
	if want := filepath.Join("..", ".flat", "deadbeef"); target != want {
		t.Fatalf("symlink target = %q, want %q", target, want)
	}
}

func TestMountlessCreateVariantSymlink(t *testing.T) {
	withMountlessPublishing(t)
	mockrepo := filepath.Clean("/" + os.Getenv("CVMFS_TEST_REPO"))

	// Variant symlinks store their target verbatim (including $(...) exprs).
	const rawTarget = "$(CVMFS_ARCH:-amd64)/rootfs"
	if err := CreateVariantSymlinkIntoCVMFS(".."+mockrepo, "variants/multiarch", rawTarget); err != nil {
		t.Fatalf("mountless CreateVariantSymlinkIntoCVMFS failed: %v", err)
	}

	linkPath := filepath.Join(mockrepo, "variants", "multiarch")
	target, err := os.Readlink(linkPath)
	if err != nil {
		t.Fatalf("variant symlink not created at %s: %v", linkPath, err)
	}
	if target != rawTarget {
		t.Fatalf("variant symlink target = %q, want %q", target, rawTarget)
	}
}

func TestMountlessCreateCatalog(t *testing.T) {
	withMountlessPublishing(t)
	mockrepo := filepath.Clean("/" + os.Getenv("CVMFS_TEST_REPO"))

	if err := CreateCatalogIntoDir(".."+mockrepo, "catalogdir/nested"); err != nil {
		t.Fatalf("mountless CreateCatalogIntoDir failed: %v", err)
	}

	catalog := filepath.Join(mockrepo, "catalogdir", "nested", ".cvmfscatalog")
	if _, err := os.Stat(catalog); err != nil {
		t.Fatalf("expected .cvmfscatalog at %s: %v", catalog, err)
	}
}
