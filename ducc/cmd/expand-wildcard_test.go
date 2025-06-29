package cmd

import (
	"testing"

	"github.com/cvmfs/ducc/testutils"
)

func TestExpandWildcardTrueRemote(t *testing.T) {
	if !*testutils.Online {
		t.Skip("Skipping test in offline mode.")
	}
	var err error
	cmd := rootCmd
	cmd.SetArgs([]string{"expand-wildcard",
		"https://registry.hub.docker.com/atlas/athena:21.0.*"})
	err = cmd.Execute()
	if err != nil {
		t.Fatal(err)
	}
}

func TestExpandWildcardMockRemote(t *testing.T) {
	if !*testutils.LocalRegistry {
		t.Skip("Skipping test that needs local registry.")
	}
	var err error
	cmd := rootCmd
	cmd.SetArgs([]string{"expand-wildcard",
		testutils.GetTestRegistryUrl() + "multi-arch-test:*"})
	err = cmd.Execute()
	if err != nil {
		t.Fatal(err)
	}
}
func TestExpandWildcardMockRemote2(t *testing.T) {
	if !*testutils.LocalRegistry {
		t.Skip("Skipping test that needs local registry.")
	}
	var err error
	cmd := rootCmd
	cmd.SetArgs([]string{"expand-wildcard",
		testutils.GetTestRegistryUrl() + "nosuchimage:*"})
	err = cmd.Execute()
	if err == nil {
		t.Fatal(err)
	}
}
