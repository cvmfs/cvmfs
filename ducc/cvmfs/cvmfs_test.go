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
