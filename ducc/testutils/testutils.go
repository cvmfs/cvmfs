package testutils

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
)

var Online *bool
var LocalRegistry *bool
var MockCvmfs *bool
var TestRegistryPort = 0
var TestRegistryServer *http.Server
var TestRepo string

func GetTestRegistryUrl() string {
	return "http://localhost:" + strconv.Itoa(TestRegistryPort) + "/"
}

func AdditionalTestFlags() {

	MockCvmfs = flag.Bool("mock-cvmfs", true, "Mock cvmfs_server repo operations with local filesystem only")
	Online = flag.Bool("online", false, "Use real registries for testing")
	LocalRegistry = flag.Bool("local-registry", true, "Use local test container registry for testing")
	flag.Parse()

}

func MockCvmfsSetup() {
	if *MockCvmfs {
		wd, _ := os.Getwd()
		os.Setenv("PATH", wd+"/../cvmfs/.mockcvmfs/:"+os.Getenv("PATH"))
		mockrepo, _ := os.MkdirTemp("", "DuccMockRepo")
		os.MkdirAll(filepath.Join(mockrepo, "scratch", "current"), os.ModePerm)
		TestRepo = "/../../../../" + mockrepo
		os.Setenv("CVMFS_TEST_REPO", TestRepo)
		os.Setenv("CVMFS_DUCC_NO_CHOWN", "nochown")
	}

}

func TestRegistrySetup() {

	if *LocalRegistry {
		// Create a context that we can cancel
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var err error
		if TestRegistryServer, TestRegistryPort, err = StartTestRegistryServer(); err != nil {
			log.Fatalf("Failed to start test registry server: %v ", err)
		}
		// Create and push multi-architecture test image
		if err := CreateAndPushMultiArchTestImage(ctx, TestRegistryPort); err != nil {
			log.Fatalf("Failed to create and push multi-arch test image: %v", err)
		}
	}
}

func TestRegistryTeardown() {
	if *LocalRegistry {
		StopTestRegistryServer(TestRegistryServer)
	}
}
