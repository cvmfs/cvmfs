package cmd

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cvmfs/ducc/lib"
	"github.com/cvmfs/ducc/testutils"
)

func TestFlatLayerIDDeterministic(t *testing.T) {
	id1 := lib.FlatLayerID("abc123")
	id2 := lib.FlatLayerID("abc123")
	if id1 != id2 {
		t.Errorf("flatLayerID not deterministic: %q != %q", id1, id2)
	}
	id3 := lib.FlatLayerID("abc124")
	if id1 == id3 {
		t.Errorf("flatLayerID collision for different inputs")
	}
	if len(id1) != 64 {
		t.Errorf("flatLayerID length %d, want 64", len(id1))
	}
}

func TestDirSizeEmpty(t *testing.T) {
	dir := t.TempDir()
	sz, err := lib.DirSize(dir)
	if err != nil {
		t.Fatal(err)
	}
	if sz != 0 {
		t.Errorf("empty dir size = %d, want 0", sz)
	}
}

func TestDirSizeKnown(t *testing.T) {
	dir := t.TempDir()
	for _, pair := range []struct {
		name string
		size int
	}{
		{"a.txt", 100},
		{"sub/b.txt", 200},
	} {
		path := filepath.Join(dir, pair.name)
		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, make([]byte, pair.size), 0644); err != nil {
			t.Fatal(err)
		}
	}
	sz, err := lib.DirSize(dir)
	if err != nil {
		t.Fatal(err)
	}
	if sz != 300 {
		t.Errorf("dirSize = %d, want 300", sz)
	}
}

func TestReadOrGenerateLinkIDIdempotent(t *testing.T) {
	dir := t.TempDir()
	linkFile := filepath.Join(dir, "link")

	id1, err := readOrGenerateLinkID(linkFile)
	if err != nil {
		t.Fatal(err)
	}
	id2, err := readOrGenerateLinkID(linkFile)
	if err != nil {
		t.Fatal(err)
	}
	if id1 != id2 {
		t.Errorf("readOrGenerateLinkID not idempotent: %q != %q", id1, id2)
	}
	if len(id1) != 26 {
		t.Errorf("link ID length %d, want 26", len(id1))
	}
}

// testImageRef returns the image reference for the single-arch amd64 image
// that TestRegistrySetup pushes to the local registry.
func testImageRef() string {
	return testutils.GetTestRegistryUrl() + "multi-arch-test:amd64"
}

// TestCreatePodmanStoreMissingFlat verifies that createPodmanStore returns an
// error when the flat image directory is absent from the (mock) CVMFS mount.
// It uses the local test registry so no real internet access is needed.
func TestCreatePodmanStoreMissingFlat(t *testing.T) {
	if !*testutils.LocalRegistry {
		t.Skip("Skipping test that needs local registry.")
	}

	// Point cvmfsRoot at an empty temp dir – the flat image won't be there.
	origRoot := cvmfsRoot
	cvmfsRoot = t.TempDir()
	defer func() { cvmfsRoot = origRoot }()

	storeDir := filepath.Join(t.TempDir(), "store")
	err := createPodmanStore(
		testImageRef(),
		"no-such-repo.cern.ch",
		storeDir,
	)
	if err == nil {
		t.Error("expected error for missing flat image, got nil")
	}
}

// TestCreatePodmanStoreStructure verifies the full directory layout written by
// createPodmanStore.  A fake flat image is created under a temporary directory
// that replaces the real /cvmfs mount (via the cvmfsRoot package variable).
// The image is pulled from the local test registry so no internet is required.
func TestCreatePodmanStoreStructure(t *testing.T) {
	if !*testutils.LocalRegistry {
		t.Skip("Skipping test that needs local registry.")
	}

	imageRef := testImageRef()

	img, err := lib.ParseImage(imageRef)
	if err != nil {
		t.Fatalf("ParseImage: %v", err)
	}
	manifest, err := img.GetManifest()
	if err != nil {
		t.Fatalf("GetManifest: %v", err)
	}

	imageID := strings.TrimPrefix(manifest.Config.Digest, "sha256:")
	flatRelPath := manifest.GetSingularityPath()

	// Build a mock CVMFS root containing the flat image directory.
	fakeRoot := t.TempDir()
	const fakeRepo = "test-repo.cern.ch"
	flatAbsPath := filepath.Join(fakeRoot, fakeRepo, flatRelPath)
	if err := os.MkdirAll(filepath.Join(flatAbsPath, "etc"), 0755); err != nil {
		t.Fatalf("creating fake flat dir: %v", err)
	}
	// Write a real file so that DirSize returns a value > 0.
	if err := os.WriteFile(
		filepath.Join(flatAbsPath, "etc", "os-release"),
		[]byte("ID=test\n"), 0644,
	); err != nil {
		t.Fatalf("writing sentinel file: %v", err)
	}

	// Redirect createPodmanStore away from /cvmfs to our fake root.
	origRoot := cvmfsRoot
	cvmfsRoot = fakeRoot
	defer func() { cvmfsRoot = origRoot }()

	storeDir := filepath.Join(t.TempDir(), "podmanstore")
	if err := createPodmanStore(imageRef, fakeRepo, storeDir); err != nil {
		t.Fatalf("createPodmanStore: %v", err)
	}

	layerID := lib.FlatLayerID(imageID)

	// ---- check that all expected paths exist --------------------------------
	checks := []string{
		filepath.Join(storeDir, "overlay", layerID, "diff"),
		filepath.Join(storeDir, "overlay", layerID, "link"),
		filepath.Join(storeDir, "overlay-images", "images.json"),
		filepath.Join(storeDir, "overlay-images", "images.lock"),
		filepath.Join(storeDir, "overlay-images", imageID, "manifest"),
		filepath.Join(storeDir, "overlay-layers", "layers.json"),
		filepath.Join(storeDir, "overlay-layers", "layers.lock"),
	}
	for _, path := range checks {
		if _, statErr := os.Lstat(path); statErr != nil {
			t.Errorf("missing expected store path: %s", path)
		}
	}

	// ---- diff symlink must point back into our fake flat dir ----------------
	diffPath := filepath.Join(storeDir, "overlay", layerID, "diff")
	target, err := os.Readlink(diffPath)
	if err != nil {
		t.Fatalf("readlink diff: %v", err)
	}
	if target != flatAbsPath {
		t.Errorf("diff symlink target = %q, want %q", target, flatAbsPath)
	}

	// ---- layers.json --------------------------------------------------------
	layersRaw, err := os.ReadFile(filepath.Join(storeDir, "overlay-layers", "layers.json"))
	if err != nil {
		t.Fatal(err)
	}
	var layers []lib.LayerInfo
	if err := json.Unmarshal(layersRaw, &layers); err != nil {
		t.Fatalf("layers.json parse error: %v", err)
	}
	if len(layers) != 1 {
		t.Fatalf("layers.json: got %d entries, want 1", len(layers))
	}
	if layers[0].UncompressedDigest == "" {
		t.Error("layers.json: diff-digest must not be empty")
	}
	if layers[0].UncompressedSize <= 0 {
		t.Error("layers.json: diff-size must be > 0")
	}

	// ---- images.json --------------------------------------------------------
	imagesRaw, err := os.ReadFile(filepath.Join(storeDir, "overlay-images", "images.json"))
	if err != nil {
		t.Fatal(err)
	}
	var images []lib.ImageInfo
	if err := json.Unmarshal(imagesRaw, &images); err != nil {
		t.Fatalf("images.json parse error: %v", err)
	}
	if len(images) != 1 {
		t.Fatalf("images.json: got %d entries, want 1", len(images))
	}
	if images[0].ID != imageID {
		t.Errorf("images.json: id = %q, want %q", images[0].ID, imageID)
	}
	if images[0].Layer != layerID {
		t.Errorf("images.json: layer = %q, want %q", images[0].Layer, layerID)
	}
}
