package cmd

import (
	"os"
	"path/filepath"
	"testing"
//  "github.com/cvmfs/ducc/testutils"
  "testutils"
)



// For these tests, use a mocked cvmfs_server command that does nothing, just lets
// us copy or extract files to a normal directory
// That makes unittests more lightweight and does not require a cvmfs installation
// could go as well into init
func TestMain(m *testing.M) {
	// Setup
	wd, _ := os.Getwd()
	os.Setenv("PATH", wd+"/../cvmfs/.mockcvmfs/:"+os.Getenv("PATH"))
	mockrepo, _ := os.MkdirTemp("", "DuccMockRepo")
	os.MkdirAll(filepath.Join(mockrepo, "scratch", "current"), os.ModePerm)
	os.Setenv("CVMFS_TEST_REPO", "/../../../../"+mockrepo)
	os.Setenv("CVMFS_DUCC_NO_CHOWN", "nochown")
	// Test

  var MyTestRegistryServer *http.Server
  var err error
  if MyTestRegistryServer,  err = StartTestRegistryServer(); err != nil {
    log.Fatalf("Failed to start test registry server: %v ", err)
  }

	code := m.Run()
	// Teardown
  StopTestRegistryServer(MyTestRegistryServer)
	os.Exit(code)
}

func TestCheckDownloadManifestTrueRemote(t *testing.T) {
	t.Log("Mockrepo: ", os.Getenv("CVMFS_TEST_REPO"))
	var err error
	cmd := rootCmd
	cmd.SetArgs([]string{"download-manifest",
		"registry.hub.docker.com/library/alpine:latest"})
	err = cmd.Execute()
	if err != nil {
		t.Fatal(err)
	}
}

func TestCheckDownloadManifestMockRemote(t *testing.T) {
	t.Log("Mockrepo: ", os.Getenv("CVMFS_TEST_REPO"))
	var err error
	cmd := rootCmd
	cmd.SetArgs([]string{"download-manifest",
		"http://localhost:5000/multi-arch-test:latest"})
	err = cmd.Execute()
	if err != nil {
		t.Fatal(err)
	}
}

