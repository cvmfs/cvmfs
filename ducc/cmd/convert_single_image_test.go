package cmd

import (
	"os"
	"path/filepath"
	"testing"
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
	code := m.Run()
	// Teardown
	os.Exit(code)
}

func TestCheckConvertSingleImageCmd(t *testing.T) {
	t.Log("Mockrepo: ", os.Getenv("CVMFS_TEST_REPO"))
	var err error
	cmd := rootCmd
	cmd.SetArgs([]string{"convert-single-image",
		"registry.hub.docker.com/library/alpine:latest", "-p", "-i", os.Getenv("CVMFS_TEST_REPO")})
	err = cmd.Execute()
	if err != nil {
		t.Fatal(err)
	}
}

func TestCheckConvertSingleImageCmdShouldFail1(t *testing.T) {
	t.Log("Mockrepo: ", os.Getenv("CVMFS_TEST_REPO"))
	var err error
	cmd := rootCmd
	cmd.SetArgs([]string{"convert-single-image", "nosuchimageexists"})
	err = cmd.Execute()
	if err == nil {
		t.Fatal("That should have returned an error")
	}
	cmd.SetArgs([]string{"convert-single-image", "nosuchimageexists", os.Getenv("CVMFS_TEST_REPO")})
	err = cmd.Execute()
	if err == nil {
		t.Fatal("That should have returned an error")
	}
}

func TestCheckConvertSingleImageCmdShouldFail2(t *testing.T) {
	t.Log("Mockrepo: ", os.Getenv("CVMFS_TEST_REPO"))
	var err error
	cmd := rootCmd
	cmd.SetArgs([]string{"convert-single-image",
		"registry.hub.docker.com/nonsenseurl/doesnotexist:latest", "-p", "-i", os.Getenv("CVMFS_TEST_REPO")})
	err = cmd.Execute()
	if err == nil {
		t.Fatal("That should have returned an error")
	}
}
