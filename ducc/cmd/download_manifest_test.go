package cmd

import (
	"os"
	"testing"

	"github.com/cvmfs/ducc/testutils"
)

// For these tests, use a mocked cvmfs_server command that does nothing, just lets
// us copy or extract files to a normal directory.
// That makes unittests more lightweight and does not require a cvmfs installation.
func TestMain(m *testing.M) {
	// Setup
	testutils.AdditionalTestFlags()
	testutils.TestRegistrySetup()
	testutils.MockCvmfsSetup()
	// Run Tests
	code := m.Run()
	// Teardown
	testutils.TestRegistryTeardown()
	os.Exit(code)
}

func TestCheckDownloadManifestTrueRemote(t *testing.T) {
	if !*testutils.Online {
		t.Skip("Skipping test in offline mode.")
	}
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
	if !*testutils.LocalRegistry {
		t.Skip("Skipping test that needs local registry.")
	}
	var err error
	cmd := rootCmd
	cmd.SetArgs([]string{"download-manifest",
		testutils.GetTestRegistryUrl() + "multi-arch-test:latest"})
	err = cmd.Execute()
	if err != nil {
		t.Fatal(err)
	}
}
