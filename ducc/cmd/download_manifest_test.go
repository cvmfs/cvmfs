package cmd

import (
	"os"
  "context"
  "strconv"
	"path/filepath"
	"testing"
  "github.com/cvmfs/ducc/testutils"
  "net/http"
  "time"
  log "github.com/sirupsen/logrus"
)


var (
   TestRegistryPort int
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
     // Create a context that we can cancel
   ctx, cancel := context.WithCancel(context.Background())
   defer cancel()


  var MyTestRegistryServer *http.Server
  var err error
  if MyTestRegistryServer, TestRegistryPort,  err = testutils.StartTestRegistryServer(); err != nil {
    log.Fatalf("Failed to start test registry server: %v ", err)
  }
    // Wait a moment for the server to start
    time.Sleep(500 * time.Millisecond)
     // Create and push multi-architecture test image
   if err := testutils.CreateAndPushMultiArchTestImage(ctx, TestRegistryPort); err != nil {
       log.Fatalf("Failed to create and push multi-arch test image: %v", err)
   }


	code := m.Run()
	// Teardown
  testutils.StopTestRegistryServer(MyTestRegistryServer)
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
		"http://localhost:"+strconv.Itoa(TestRegistryPort)+"/multi-arch-test:latest"})
	err = cmd.Execute()
	if err != nil {
		t.Fatal(err)
	}
}

