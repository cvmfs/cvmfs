package cvmfs

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// For these tests, use a mocked cvmfs_server command that does nothing, just lets
// us copy or extract files to a normal directory
// That makes unittests more lightweight and does not require a cvmfs installation
// could go as well into init
func TestMain(m *testing.M) {
	// Setup
	wd, _ := os.Getwd()
	os.Setenv("PATH", wd+"/.mockcvmfs/:"+os.Getenv("PATH"))
	mockrepo, _ := os.MkdirTemp("", "DuccMockRepo")
	os.MkdirAll(filepath.Join(mockrepo, "scratch", "current"), os.ModePerm)
	os.Setenv("CVMFS_TEST_REPO", "/../../../../"+mockrepo)
	os.Setenv("CVMFS_DUCC_NO_CHOWN", "nochown")
	overlaycachedir, _ := os.MkdirTemp("", "ducc_overlaycache")
	os.Setenv("DUCC_OVERLAY_CACHE_DIR", overlaycachedir)
	// Test
	code := m.Run()
	// Teardown
	os.Exit(code)
}

// Check that we are indeed using the mocked cvmfs_server
func TestMockCommand(t *testing.T) {
	out, err := exec.Command("cvmfs_server").Output()
	if err != nil {
		t.Fatal(err)
	} else if !strings.HasPrefix(string(out), "WARNING") {
		t.Fatal(err)
	}
}

func TestPublishToCVMFS(t *testing.T) {
	mockrepo := filepath.Clean("/" + os.Getenv("CVMFS_TEST_REPO"))
	t.Log("Mockrepo:", mockrepo)

	// test publish file
	f, _ := os.CreateTemp("", "PublishTestFile")
	t.Log("Testfile:", f.Name())
	testfile1Content := []byte("testfile1content")
	f.Write(testfile1Content)
	PublishToCVMFS(".."+mockrepo, "testfile1", f.Name())
	testfile1Readback, err := os.ReadFile(mockrepo + "/testfile1")
	if err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(testfile1Readback, testfile1Content) {
		t.Fatal("Published file on CVMFS differs!")
	}

	// test publish dir
	d, _ := os.MkdirTemp("", "PublishTestDir")
	t.Log(d)
	f2, _ := os.CreateTemp(d, "PublishTestFile2")
	t.Log("Testfile2:", f2.Name())
	testfile2Content := []byte("testfile2content")
	f2.Write(testfile2Content)
	PublishToCVMFS(".."+mockrepo, "subdir2", d)
	testfile2Readback, err := os.ReadFile(filepath.Join(mockrepo, "subdir2", filepath.Base(f2.Name())))
	if err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(testfile2Readback, testfile2Content) {
		t.Fatal("Published file on CVMFS differs!")
	}
}

func TestOverlayEmptyLayers(t *testing.T) {
	err := Overlay("testrepo", []string{}, "/dest", "")
	if err == nil {
		t.Fatal("Expected error for empty layer paths, got nil")
	}
	if !strings.Contains(err.Error(), "no layer paths provided") {
		t.Fatalf("Unexpected error message: %s", err.Error())
	}
}

func TestOverlayWithoutCacheDir(t *testing.T) {
	mockrepo := filepath.Clean("/" + os.Getenv("CVMFS_TEST_REPO"))
	repoName := ".." + mockrepo

	destPath := "overlay_test_no_cache"
	err := Overlay(repoName, []string{"layer1", "layer2"}, destPath, "")
	if err != nil {
		t.Fatalf("Overlay failed: %s", err)
	}

	// Verify the mock created the destination directory with a marker file
	markerPath := filepath.Join(mockrepo, destPath, ".overlay_marker")
	markerContent, err := os.ReadFile(markerPath)
	if err != nil {
		t.Fatalf("Failed to read overlay marker: %s", err)
	}
	content := string(markerContent)
	if !strings.Contains(content, "layers=layer1,layer2") {
		t.Fatalf("Marker file missing expected layers, got: %s", content)
	}
	if !strings.Contains(content, "cache_dir=\n") {
		t.Fatalf("Marker file should have empty cache_dir, got: %s", content)
	}
}

func TestOverlayWithCacheDir(t *testing.T) {
	mockrepo := filepath.Clean("/" + os.Getenv("CVMFS_TEST_REPO"))
	repoName := ".." + mockrepo

	// Use a temp directory for the cache
	cacheDir, err := os.MkdirTemp("", "overlay-cache-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %s", err)
	}
	defer os.RemoveAll(cacheDir)

	destPath := "overlay_test_with_cache"
	err = Overlay(repoName, []string{"layerA", "layerB", "layerC"}, destPath, cacheDir)
	if err != nil {
		t.Fatalf("Overlay failed: %s", err)
	}

	// Verify the mock created the destination directory with a marker file
	markerPath := filepath.Join(mockrepo, destPath, ".overlay_marker")
	markerContent, err := os.ReadFile(markerPath)
	if err != nil {
		t.Fatalf("Failed to read overlay marker: %s", err)
	}
	content := string(markerContent)
	if !strings.Contains(content, "layers=layerA,layerB,layerC") {
		t.Fatalf("Marker file missing expected layers, got: %s", content)
	}
	if !strings.Contains(content, "cache_dir="+cacheDir) {
		t.Fatalf("Marker file missing expected cache_dir, got: %s", content)
	}
}

func TestOverlayCacheDirCreated(t *testing.T) {
	mockrepo := filepath.Clean("/" + os.Getenv("CVMFS_TEST_REPO"))
	repoName := ".." + mockrepo

	// Use a non-existent subdirectory as cache dir
	tmpDir, err := os.MkdirTemp("", "overlay-cache-parent")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %s", err)
	}
	defer os.RemoveAll(tmpDir)

	cacheDir := filepath.Join(tmpDir, "nested", "cache", "dir")

	// Verify it doesn't exist yet
	if _, err := os.Stat(cacheDir); !os.IsNotExist(err) {
		t.Fatal("Cache dir should not exist before Overlay call")
	}

	destPath := "overlay_test_cache_created"
	err = Overlay(repoName, []string{"layer1"}, destPath, cacheDir)
	if err != nil {
		t.Fatalf("Overlay failed: %s", err)
	}

	// Verify the cache directory was created
	info, err := os.Stat(cacheDir)
	if err != nil {
		t.Fatalf("Cache dir was not created: %s", err)
	}
	if !info.IsDir() {
		t.Fatal("Cache dir path is not a directory")
	}
}
