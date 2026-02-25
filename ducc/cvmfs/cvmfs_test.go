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

func TestPublishToSubdir(t *testing.T) {
	mockrepo := filepath.Clean("/" + os.Getenv("CVMFS_TEST_REPO"))
	t.Log("Mockrepo:", mockrepo)

	// Create a subdirectory in the mock repo to simulate a pre-existing state or just target it
	subdirName := "target_subdir"
	fullRepoPath := ".." + mockrepo + ":" + subdirName

	f, _ := os.CreateTemp("", "PublishSubdirTestFile")
	t.Log("SubdirTestFile:", f.Name())
	content := []byte("subdir_test_content")
	f.Write(content)

	// Publish to a sub-path within the subdirectory
	// Logic: ducc should extract 'mockrepo' as the scratch base, but put files into 'subdirName/deep/path'
	err := PublishToCVMFS(fullRepoPath, "deep/path/file", f.Name())
	if err != nil {
		t.Fatal(err)
	}

	// Verification
	// The file should exist at mockrepo/subdirName/deep/path/file
	expectedPath := filepath.Join(mockrepo, subdirName, "deep", "path", "file")
	readback, err := os.ReadFile(expectedPath)
	if err != nil {
		t.Fatalf("Failed to read back file from expected path %s: %v", expectedPath, err)
	}
	if !bytes.Equal(readback, content) {
		t.Fatal("Published file content differs!")
	}
}

func TestPublishToSubdirAlreadyPrefixedPath(t *testing.T) {
	mockrepo := filepath.Clean("/" + os.Getenv("CVMFS_TEST_REPO"))
	subdirName := "prefixed_target_subdir"
	fullRepoPath := ".." + mockrepo + ":" + subdirName

	f, err := os.CreateTemp("", "PublishSubdirPrefixedPathTestFile")
	if err != nil {
		t.Fatal(err)
	}
	content := []byte("prefixed_subdir_test_content")
	if _, err := f.Write(content); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	alreadyPrefixedPath := filepath.Join(subdirName, "deep", "path", "file")
	if err := PublishToCVMFS(fullRepoPath, alreadyPrefixedPath, f.Name()); err != nil {
		t.Fatal(err)
	}

	expectedPath := filepath.Join(mockrepo, subdirName, "deep", "path", "file")
	readback, err := os.ReadFile(expectedPath)
	if err != nil {
		t.Fatalf("Failed to read expected file %s: %v", expectedPath, err)
	}
	if !bytes.Equal(readback, content) {
		t.Fatal("Published file content differs!")
	}

	duplicatedPath := filepath.Join(mockrepo, subdirName, subdirName, "deep", "path", "file")
	if _, err := os.Stat(duplicatedPath); err == nil {
		t.Fatalf("Unexpected duplicated subdir path created: %s", duplicatedPath)
	}
}

func TestCreateCatalogIntoDirSubdir(t *testing.T) {
	mockrepo := filepath.Clean("/" + os.Getenv("CVMFS_TEST_REPO"))
	subdirName := "catalog_target_subdir"
	fullRepoPath := ".." + mockrepo + ":" + subdirName
	dir := filepath.Join("catalog_test", "nested")

	if err := CreateCatalogIntoDir(fullRepoPath, dir); err != nil {
		t.Fatal(err)
	}

	expectedPath := filepath.Join(mockrepo, subdirName, dir, ".cvmfscatalog")
	if _, err := os.Stat(expectedPath); err != nil {
		t.Fatalf("Expected catalog file missing at %s: %v", expectedPath, err)
	}

	wrongRootPath := filepath.Join(mockrepo, dir, ".cvmfscatalog")
	if _, err := os.Stat(wrongRootPath); err == nil {
		t.Fatalf("Catalog file was unexpectedly created in repository root: %s", wrongRootPath)
	}
}

func TestIngestDeleteSubdirPathHandling(t *testing.T) {
	mockrepo := filepath.Clean("/" + os.Getenv("CVMFS_TEST_REPO"))
	subdirName := "delete_target_subdir"
	fullRepoPath := ".." + mockrepo + ":" + subdirName

	tests := []struct {
		name         string
		deletePathFn func(repoRelativePath string) string
	}{
		{
			name: "unprefixed_path",
			deletePathFn: func(repoRelativePath string) string {
				return repoRelativePath
			},
		},
		{
			name: "already_prefixed_path",
			deletePathFn: func(repoRelativePath string) string {
				return filepath.Join(subdirName, repoRelativePath)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repoRelativePath := filepath.Join("delete_test", tt.name, "file")
			target, err := os.CreateTemp("", "DeleteSubdirTestFile")
			if err != nil {
				t.Fatal(err)
			}
			content := []byte("delete_subdir_test_content")
			if _, err := target.Write(content); err != nil {
				t.Fatal(err)
			}
			if err := target.Close(); err != nil {
				t.Fatal(err)
			}

			if err := PublishToCVMFS(fullRepoPath, repoRelativePath, target.Name()); err != nil {
				t.Fatal(err)
			}

			expectedPath := filepath.Join(mockrepo, subdirName, repoRelativePath)
			if _, err := os.Stat(expectedPath); err != nil {
				t.Fatalf("Expected test file missing at %s: %v", expectedPath, err)
			}

			deletePath := tt.deletePathFn(repoRelativePath)
			if err := IngestDelete(fullRepoPath, deletePath); err != nil {
				t.Fatal(err)
			}

			if _, err := os.Stat(expectedPath); !os.IsNotExist(err) {
				t.Fatalf("Expected file to be deleted at %s, stat error: %v", expectedPath, err)
			}
		})
	}
}

func TestOverlayEmptyLayers(t *testing.T) {
	err := Overlay("testrepo", []string{}, "/dest")
	if err == nil {
		t.Fatal("Expected error for empty layer paths, got nil")
	}
	if !strings.Contains(err.Error(), "no layer paths provided") {
		t.Fatalf("Unexpected error message: %s", err.Error())
	}
}

func TestOverlay(t *testing.T) {
	mockrepo := filepath.Clean("/" + os.Getenv("CVMFS_TEST_REPO"))
	repoName := ".." + mockrepo

	destPath := "overlay_test"
	err := Overlay(repoName, []string{"layer1", "layer2"}, destPath)
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
}
