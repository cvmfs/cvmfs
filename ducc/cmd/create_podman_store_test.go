package cmd

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/cvmfs/ducc/lib"
	"github.com/cvmfs/ducc/testutils"
)

// TestFlatLayerIDDeterministic verifies that flatLayerID always produces the
// same output for the same input and that distinct inputs produce distinct IDs.
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
	// Must be exactly 64 hex chars (sha256)
	if len(id1) != 64 {
		t.Errorf("flatLayerID length %d, want 64", len(id1))
	}
}

// TestDirSizeEmpty verifies that an empty directory has size 0.
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

// TestDirSizeKnown creates files of known sizes and checks the total.
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

// TestReadOrGenerateLinkIDIdempotent checks that reading an existing link file
// returns the same ID without regenerating it.
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

// TestCreatePodmanStoreMissingFlat verifies that createPodmanStore returns an
// error when the flat image does not exist on the CVMFS mount, without
// requiring a running CVMFS instance.
func TestCreatePodmanStoreMissingFlat(t *testing.T) {
	if !*testutils.Online {
		t.Skip("Skipping test in offline mode.")
	}
	storeDir := filepath.Join(t.TempDir(), "store")
	err := createPodmanStore(
		"https://registry.hub.docker.com/library/alpine:latest",
		"no-such-repo.cern.ch",
		storeDir,
	)
	if err == nil {
		t.Error("expected error for missing flat image, got nil")
	}
}

// TestCreatePodmanStoreStructure runs the full command against a real image and
// a pre-existing flat directory, then checks that every required store file is
// present and well-formed.
func TestCreatePodmanStoreStructure(t *testing.T) {
	if !*testutils.Online {
		t.Skip("Skipping test in offline mode.")
	}

	// Build a minimal fake flat directory that looks like a CVMFS flat image.
	// createPodmanStore only checks that the path exists via os.Stat; it does
	// not inspect its contents.
	imageRef := "https://registry.hub.docker.com/library/alpine:latest"

	img, err := lib.ParseImage(imageRef)
	if err != nil {
		t.Fatalf("ParseImage: %v", err)
	}
	manifest, err := img.GetManifest()
	if err != nil {
		t.Fatalf("GetManifest: %v", err)
	}

	// Construct the expected flat path and create it as a temp dir so
	// createPodmanStore's os.Stat check passes.
	flatRelPath := manifest.GetSingularityPath()
	fakeRepo := "fake-cvmfs-test.cern.ch"
	fakeCVMFSRoot := t.TempDir()
	flatAbsPath := filepath.Join(fakeCVMFSRoot, "cvmfs", fakeRepo, flatRelPath)
	if err := os.MkdirAll(flatAbsPath, 0755); err != nil {
		t.Fatalf("creating fake flat dir: %v", err)
	}
	// Write a small file so dirSize returns a non-zero value.
	if err := os.WriteFile(filepath.Join(flatAbsPath, "etc", "os-release"),
		[]byte("ID=alpine\n"), 0644); err != nil {
		if mkErr := os.MkdirAll(filepath.Join(flatAbsPath, "etc"), 0755); mkErr != nil {
			t.Fatal(mkErr)
		}
		if err := os.WriteFile(filepath.Join(flatAbsPath, "etc", "os-release"),
			[]byte("ID=alpine\n"), 0644); err != nil {
			t.Fatal(err)
		}
	}

	// Patch the flat path lookup: createPodmanStore builds the path as
	// filepath.Join("/cvmfs", repoName, flatRelPath).  We can't easily redirect
	// "/cvmfs" in tests, so we override it by symlinking.
	// Instead, re-implement the store creation inline against our fake root.
	storeDir := filepath.Join(t.TempDir(), "podmanstore")

	// Use the cobra command directly so we exercise the full code path through
	// the registered flags and RunE.
	// Because the flat-path check is hard-wired to /cvmfs, we skip the
	// end-to-end command test and call the helper directly after overriding
	// the flat root via a symlink trick.
	//
	// Create /tmp/cvmfs symlink pointing to our fake root, if writable.
	// If that isn't possible in this environment, skip.
	fakeMount := filepath.Join(fakeCVMFSRoot, "cvmfs")
	realCVMFS := "/cvmfs"
	savedReal := ""
	if info, statErr := os.Lstat(realCVMFS); statErr == nil && info.IsDir() {
		// /cvmfs exists – we can't override it; exercise what we can.
		t.Logf("/cvmfs already exists; skipping store-creation end-to-end sub-test")
	} else {
		// /cvmfs doesn't exist in this environment; create a symlink
		savedReal = realCVMFS
		if symlinkErr := os.Symlink(fakeMount, realCVMFS); symlinkErr != nil {
			t.Skipf("cannot create /cvmfs symlink (%v); skipping end-to-end", symlinkErr)
		}
		defer os.Remove(savedReal)
	}
	_ = savedReal

	if err := createPodmanStore(imageRef, fakeRepo, storeDir); err != nil {
		t.Fatalf("createPodmanStore: %v", err)
	}

	// Verify required files.
	imageID := func() string {
		s := manifest.Config.Digest
		if len(s) > 7 {
			return s[7:]
		}
		return s
	}()
	layerID := lib.FlatLayerID(imageID)

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
			t.Errorf("missing expected store file: %s", path)
		}
	}

	// Validate layers.json has diff-digest and diff-size set.
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

	// Validate images.json references the layer and image.
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
