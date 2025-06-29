package testutils

import (
	"context"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
)

var Online *bool = new(bool)
var LocalRegistry *bool = new(bool)
var MockCvmfs *bool = new(bool)
var TestRegistryPort = 0
var TestRegistryServer *http.Server
var TestRepo string

func GetTestRegistryUrl() string {
	return "http://localhost:" + strconv.Itoa(TestRegistryPort) + "/"
}

func AdditionalTestFlags() {

	*MockCvmfs = (os.Getenv("TEST_DUCC_NOMOCK") == "")
	*Online = (os.Getenv("TEST_DUCC_ONLINE") == "")
	*LocalRegistry = !(os.Getenv("TEST_DUCC_NOLOCALREGISTRY") == "")
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
