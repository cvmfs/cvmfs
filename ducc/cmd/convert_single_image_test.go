package cmd

import (
	"github.com/cvmfs/ducc/testutils"
	"testing"
)

func TestCheckConvertSingleImageCmdDockerhub(t *testing.T) {
	if !*testutils.Online {
		t.Skip("Skipping test in offline mode.")
	}
	var err error
	cmd := rootCmd
	cmd.SetArgs([]string{"convert-single-image",
		"registry.hub.docker.com/library/alpine:latest", "-p", "-i", testutils.TestRepo})
	err = cmd.Execute()
	if err != nil {
		t.Fatal(err)
	}
}

func TestCheckConvertSingleImageLocal(t *testing.T) {
	if !*testutils.LocalRegistry {
		t.Skip("Skipping test that needs local registry.")
	}
	var err error
	cmd := rootCmd
	cmd.SetArgs([]string{"convert-single-image",
		testutils.GetTestRegistryUrl() + "multi-arch-test:latest", "-p", "-i", testutils.TestRepo})
	err = cmd.Execute()
	if err != nil {
		t.Fatal(err)
	}
}

func TestCheckConvertSingleImageCmdShouldFail1(t *testing.T) {
	var err error
	cmd := rootCmd
	cmd.SetArgs([]string{"convert-single-image", "nosuchimageexists"})
	err = cmd.Execute()
	if err == nil {
		t.Fatal("That should have returned an error")
	}
	cmd.SetArgs([]string{"convert-single-image", "nosuchimageexists", testutils.TestRepo})
	err = cmd.Execute()
	if err == nil {
		t.Fatal("That should have returned an error")
	}
}

func TestCheckConvertSingleImageCmdShouldFail2(t *testing.T) {
	var err error
	cmd := rootCmd
	cmd.SetArgs([]string{"convert-single-image",
		"registry.hub.docker.com/nonsenseurl/doesnotexist:latest", "-p", "-i", testutils.TestRepo})
	err = cmd.Execute()
	if err == nil {
		t.Fatal("That should have returned an error")
	}
}
