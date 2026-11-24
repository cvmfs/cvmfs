package cmd

import (
	"github.com/cvmfs/ducc/testutils"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCheckConvertSingleImageCmdDockerhub(t *testing.T) {
	if !*testutils.Online {
		t.Skip("Skipping test in offline mode.")
	}
	var err error
	cmd := rootCmd
	cmd.SetArgs([]string{"convert-single-image",
		"registry.hub.docker.com/library/alpine:latest", "-i", testutils.TestRepo})
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
		testutils.GetTestRegistryUrl() + "multi-arch-test:latest", "-i", testutils.TestRepo})
	err = cmd.Execute()
	if err != nil {
		t.Fatal(err)
	}
}

func TestCheckConvertSingleImageCmdDockerhubMultiArch(t *testing.T) {
	if !*testutils.Online {
		t.Skip("Skipping test in offline mode.")
	}
	var err error
	cmd := rootCmd
	cmd.SetArgs([]string{"convert-single-image", "-m",
		"registry.hub.docker.com/library/alpine:latest",
		"-i", testutils.TestRepo})
	err = cmd.Execute()
	if err != nil {
		t.Fatal(err)
	}
}

func TestCheckConvertSingleImageLocalMultiArch(t *testing.T) {
	if !*testutils.LocalRegistry {
		t.Skip("Skipping test that needs local registry.")
	}
	var err error
	cmd := rootCmd
	cmd.SetArgs([]string{"convert-single-image", "-m",
		testutils.GetTestRegistryUrl() + "multi-arch-test:latest",
		"-i", testutils.TestRepo})
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
	if !*testutils.Online {
		t.Skip("Skipping test in offline mode.")
	}
	var err error
	cmd := rootCmd
	cmd.SetArgs([]string{"convert-single-image",
		"registry.hub.docker.com/nonsenseurl/doesnotexist:latest", "-i", testutils.TestRepo})
	err = cmd.Execute()
	if err == nil {
		t.Fatal("That should have returned an error")
	}
}

// With --multi-arch, a plain image (not an index) must keep its direct link to
// the flat image instead of a link into .multiarch/, where nothing is published.
func TestCheckConvertSingleImageLocalMultiArchPlainImage(t *testing.T) {
	if !*testutils.LocalRegistry {
		t.Skip("Skipping test that needs local registry.")
	}
	image := testutils.GetTestRegistryUrl() + "multi-arch-test:amd64"
	cmd := rootCmd
	cmd.SetArgs([]string{"convert-single-image", "-m", image, "-i", testutils.TestRepo})
	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join("/cvmfs", testutils.TestRepo, strings.TrimPrefix(image, "http://"))
	target, err := os.Readlink(link)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(target, ".multiarch") {
		t.Fatalf("%s links into .multiarch: %s", link, target)
	}
}
