package lib

import (
	"os"
	"path/filepath"
	"testing"
)

// testMockRepo is the actual absolute path to the temporary CVMFS mock
// repository directory created by TestMain.
var testMockRepo string

// TestMain sets up the mock cvmfs_server (from cvmfs/.mockcvmfs/) in PATH and
// creates a temporary directory that stands in for a real CVMFS repository.
// This is the same mechanism used in cvmfs/cvmfs_test.go.
func TestMain(m *testing.M) {
	wd, _ := os.Getwd()
	os.Setenv("PATH", wd+"/../cvmfs/.mockcvmfs/:"+os.Getenv("PATH"))

	mockrepo, _ := os.MkdirTemp("", "DuccMockRepo")
	os.MkdirAll(filepath.Join(mockrepo, "scratch", "current"), os.ModePerm)

	testMockRepo = mockrepo
	// CVMFS_TEST_REPO is read by the mock cvmfs_server script to locate the
	// repository root.  The leading /../../../../ makes filepath.Join resolve
	// the path back to the real tmp directory when combined with /cvmfs/.
	os.Setenv("CVMFS_TEST_REPO", "/../../../../"+mockrepo)
	os.Setenv("CVMFS_DUCC_NO_CHOWN", "nochown")

	code := m.Run()
	os.RemoveAll(mockrepo)
	os.Exit(code)
}

// mockCVMFSRepo returns the CVMFSRepo string to pass into lib functions so
// that all constructed paths (e.g. /cvmfs/<repo>/...) resolve to the
// temporary mock repository directory on the real filesystem.
//
// The trick:  filepath.Join("/", "cvmfs", "../../../../../tmp/mockXXX", ...)
// cleans to /tmp/mockXXX/... because .. steps cancel the /cvmfs prefix.
func mockCVMFSRepo() string {
	return ".." + os.Getenv("CVMFS_TEST_REPO")
}

// makeTestImage is a helper that constructs an Image with fixed-tag fields
// without hitting the network.
func makeTestImage(registry, repository, tag string) Image {
	return Image{
		Scheme:     "https",
		Registry:   registry,
		Repository: repository,
		Tag:        tag,
	}
}

// --------------------------------------------------------------------------
// DeleteImageFromCVMFS
// --------------------------------------------------------------------------

func TestDeleteImageFromCVMFS_ImageNotFound(t *testing.T) {
	img := makeTestImage("registry.example.com", "notexist/repo", "latest")
	CVMFSRepo := mockCVMFSRepo()

	deleted, err := DeleteImageFromCVMFS(CVMFSRepo, &img, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if deleted {
		t.Error("expected deleted=false when image does not exist in CVMFS")
	}
}

func TestDeleteImageFromCVMFS_DeletesSymlinkAndManifestDir(t *testing.T) {
	img := makeTestImage("registry.example.com", "library/alpine", "3.18")
	CVMFSRepo := mockCVMFSRepo()

	// Create the symlink path (as a regular file – Lstat treats it the same
	// as a symlink for existence purposes) and the manifest directory.
	symlinkPath := filepath.Join(testMockRepo, img.GetPublicSymlinkPath())
	manifestDir := filepath.Join(testMockRepo, ".metadata", img.GetSimpleName())

	if err := os.MkdirAll(filepath.Dir(symlinkPath), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(symlinkPath, []byte("target"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(manifestDir, 0o755); err != nil {
		t.Fatal(err)
	}
	// Place a file inside to verify the whole tree is removed.
	if err := os.WriteFile(filepath.Join(manifestDir, "manifest.json"), []byte("{}"), 0o644); err != nil {
		t.Fatal(err)
	}

	deleted, err := DeleteImageFromCVMFS(CVMFSRepo, &img, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !deleted {
		t.Error("expected deleted=true")
	}

	if _, err := os.Lstat(symlinkPath); !os.IsNotExist(err) {
		t.Error("symlink path was not removed")
	}
	if _, err := os.Stat(manifestDir); !os.IsNotExist(err) {
		t.Error("manifest dir was not removed")
	}
}

func TestDeleteImageFromCVMFS_DryRunDoesNotDelete(t *testing.T) {
	img := makeTestImage("registry.example.com", "library/ubuntu", "22.04")
	CVMFSRepo := mockCVMFSRepo()

	symlinkPath := filepath.Join(testMockRepo, img.GetPublicSymlinkPath())
	manifestDir := filepath.Join(testMockRepo, ".metadata", img.GetSimpleName())

	if err := os.MkdirAll(filepath.Dir(symlinkPath), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(symlinkPath, []byte("target"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(manifestDir, 0o755); err != nil {
		t.Fatal(err)
	}

	deleted, err := DeleteImageFromCVMFS(CVMFSRepo, &img, true /* dryRun */)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !deleted {
		t.Error("expected deleted=true (dry-run still reports what would be deleted)")
	}

	// Files must still exist after a dry run.
	if _, err := os.Lstat(symlinkPath); os.IsNotExist(err) {
		t.Error("symlink was removed during dry-run")
	}
	if _, err := os.Stat(manifestDir); os.IsNotExist(err) {
		t.Error("manifest dir was removed during dry-run")
	}
}

func TestDeleteImageFromCVMFS_DeletesMultiarchPaths(t *testing.T) {
	img := makeTestImage("registry.example.com", "library/nginx", "1.25")
	CVMFSRepo := mockCVMFSRepo()

	// Create .multiarch/<arch>/<publicSymlinkPath> entries for two arches.
	multiarchBase := filepath.Join(testMockRepo, ".multiarch")
	relPath := img.GetPublicSymlinkPath()
	for _, arch := range []string{"amd64", "arm64:v8"} {
		p := filepath.Join(multiarchBase, arch, relPath)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte("target"), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	// Also create the corresponding .metadata/.multiarch entries.
	simpleName := img.GetSimpleName()
	multiarchMetaBase := filepath.Join(testMockRepo, ".metadata", ".multiarch")
	for _, arch := range []string{"amd64", "arm64:v8"} {
		p := filepath.Join(multiarchMetaBase, arch, simpleName)
		if err := os.MkdirAll(p, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(p, "manifest.json"), []byte("{}"), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	deleted, err := DeleteImageFromCVMFS(CVMFSRepo, &img, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !deleted {
		t.Error("expected deleted=true")
	}

	for _, arch := range []string{"amd64", "arm64:v8"} {
		p := filepath.Join(multiarchBase, arch, relPath)
		if _, err := os.Lstat(p); !os.IsNotExist(err) {
			t.Errorf(".multiarch symlink not removed for arch %s", arch)
		}
		metaP := filepath.Join(multiarchMetaBase, arch, simpleName)
		if _, err := os.Stat(metaP); !os.IsNotExist(err) {
			t.Errorf(".metadata/.multiarch dir not removed for arch %s", arch)
		}
	}
}

// --------------------------------------------------------------------------
// FindCVMFSImagesMatchingPattern
// --------------------------------------------------------------------------

// setupMetadataEntry creates a .metadata/<registry>/<repo>:<tag> directory
// in the mock repo to simulate a converted image.
func setupMetadataEntry(t *testing.T, registry, repository, tag string) {
	t.Helper()
	simpleName := registry + "/" + repository + ":" + tag
	dir := filepath.Join(testMockRepo, ".metadata", simpleName)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
}

func TestFindCVMFSImagesMatchingPattern_WildcardTagFindsAll(t *testing.T) {
	CVMFSRepo := mockCVMFSRepo()
	registry := "find.example.com"
	repo := "library/busybox"

	for _, tag := range []string{"1.34", "1.35", "1.36"} {
		setupMetadataEntry(t, registry, repo, tag)
	}

	imgs, err := FindCVMFSImagesMatchingPattern(CVMFSRepo, registry, repo, "*")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(imgs) != 3 {
		t.Fatalf("expected 3 images, got %d", len(imgs))
	}
}

func TestFindCVMFSImagesMatchingPattern_GlobTagPattern(t *testing.T) {
	CVMFSRepo := mockCVMFSRepo()
	registry := "glob.example.com"
	repo := "library/debian"

	for _, tag := range []string{"10", "11", "12", "bookworm"} {
		setupMetadataEntry(t, registry, repo, tag)
	}

	// Only tags starting with "1": 10, 11, 12 — not "bookworm".
	imgs, err := FindCVMFSImagesMatchingPattern(CVMFSRepo, registry, repo, "1*")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(imgs) != 3 {
		t.Fatalf("expected 3 images matching '1*', got %d", len(imgs))
	}
}

func TestFindCVMFSImagesMatchingPattern_ExactTag(t *testing.T) {
	CVMFSRepo := mockCVMFSRepo()
	registry := "exact.example.com"
	repo := "team/myapp"

	setupMetadataEntry(t, registry, repo, "v1.0")
	setupMetadataEntry(t, registry, repo, "v2.0")

	imgs, err := FindCVMFSImagesMatchingPattern(CVMFSRepo, registry, repo, "v1.0")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(imgs) != 1 {
		t.Fatalf("expected 1 image, got %d", len(imgs))
	}
	if imgs[0].Tag != "v1.0" {
		t.Errorf("expected tag v1.0, got %q", imgs[0].Tag)
	}
}

func TestFindCVMFSImagesMatchingPattern_RegistryDoesNotExist(t *testing.T) {
	CVMFSRepo := mockCVMFSRepo()

	imgs, err := FindCVMFSImagesMatchingPattern(CVMFSRepo, "nonexistent.example.com", "lib/img", "*")
	if err != nil {
		t.Fatalf("unexpected error for missing registry: %v", err)
	}
	if len(imgs) != 0 {
		t.Errorf("expected 0 images for missing registry, got %d", len(imgs))
	}
}

func TestFindCVMFSImagesMatchingPattern_NestedRepository(t *testing.T) {
	CVMFSRepo := mockCVMFSRepo()
	registry := "nested.example.com"
	repo := "org/team/app"

	setupMetadataEntry(t, registry, repo, "stable")

	imgs, err := FindCVMFSImagesMatchingPattern(CVMFSRepo, registry, repo, "*")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(imgs) != 1 {
		t.Fatalf("expected 1 image, got %d", len(imgs))
	}
	if imgs[0].Repository != repo {
		t.Errorf("expected repository %q, got %q", repo, imgs[0].Repository)
	}
}

// --------------------------------------------------------------------------
// findMultiarchImagePaths / findMultiarchMetadataDirs
// --------------------------------------------------------------------------

func TestFindMultiarchImagePaths_ReturnsMatchingPaths(t *testing.T) {
	img := makeTestImage("ma.example.com", "team/proj", "edge")
	multiarchBase := filepath.Join(testMockRepo, ".multiarch-findtest")
	relPath := img.GetPublicSymlinkPath()

	for _, arch := range []string{"amd64", "arm64:v8"} {
		p := filepath.Join(multiarchBase, arch, relPath)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte("target"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	// Add an arch dir that does NOT contain this image.
	other := filepath.Join(multiarchBase, "386", "other.example.com", "other:tag")
	if err := os.MkdirAll(filepath.Dir(other), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(other, []byte("target"), 0o644); err != nil {
		t.Fatal(err)
	}

	paths := findMultiarchImagePaths(multiarchBase, &img)
	if len(paths) != 2 {
		t.Fatalf("expected 2 paths, got %d: %v", len(paths), paths)
	}
}

func TestFindMultiarchImagePaths_EmptyBaseDir(t *testing.T) {
	img := makeTestImage("ma.example.com", "team/proj", "missing")
	paths := findMultiarchImagePaths(filepath.Join(testMockRepo, "nonexistent-multiarch"), &img)
	if paths != nil {
		t.Errorf("expected nil for non-existent base dir, got %v", paths)
	}
}

func TestFindMultiarchMetadataDirs_ReturnsMatchingDirs(t *testing.T) {
	img := makeTestImage("meta.example.com", "team/app", "nightly")
	metadataBase := filepath.Join(testMockRepo, ".metadata-findtest")
	simpleName := img.GetSimpleName()

	for _, arch := range []string{"amd64", "arm64:v8"} {
		p := filepath.Join(metadataBase, ".multiarch", arch, simpleName)
		if err := os.MkdirAll(p, 0o755); err != nil {
			t.Fatal(err)
		}
	}

	dirs := findMultiarchMetadataDirs(metadataBase, &img)
	if len(dirs) != 2 {
		t.Fatalf("expected 2 dirs, got %d: %v", len(dirs), dirs)
	}
}

func TestFindMultiarchMetadataDirs_EmptyBaseDir(t *testing.T) {
	img := makeTestImage("meta.example.com", "team/app", "missing")
	dirs := findMultiarchMetadataDirs(filepath.Join(testMockRepo, "nonexistent-metadata"), &img)
	if dirs != nil {
		t.Errorf("expected nil for non-existent base dir, got %v", dirs)
	}
}
