package lib

import (
	"io/ioutil"
	"os"
	"path/filepath"

	"testing"
)

func TestIsWhiteout(t *testing.T) {
	noWhiteout := "/foo/bar/noWhiteout.go"
	if isWhiteout(noWhiteout) {
		t.Errorf("%s should not be a whiteout file", noWhiteout)
	}
	whiteOut := "/foo/bar/.wh.foo.ch"
	if !isWhiteout(whiteOut) {
		t.Errorf("%s should be a whiteout file", whiteOut)
	}
	noWhiteoutNoDir := "bar.txt.wh.h"
	if isWhiteout(noWhiteoutNoDir) {
		t.Errorf("%s should not be a whiteout", noWhiteoutNoDir)
	}
	whiteOutNoDir := ".wh.bar.txt"
	if !isWhiteout(whiteOutNoDir) {
		t.Errorf("%s should ne a whiteout", whiteOutNoDir)
	}
}

func TestApplyDirectory(t *testing.T) {
	bottom, err := ioutil.TempDir("", "bottom")
	if err != nil {
		t.Errorf("Error in creating bottom directory: %s", bottom)
	}
	//defer os.RemoveAll(bottom)
	err = os.Chmod(bottom, 0766)
	if err != nil {
		t.Errorf("Error in making the bottom directory open to write: %s", bottom)
	}

	top, err := ioutil.TempDir("", "top")
	if err != nil {
		t.Errorf("Error in creating the top directory: %s", top)
	}
	//defer os.RemoveAll(top)

	os.Mkdir(filepath.Join(bottom, "a"), 0766)
	os.Create(filepath.Join(bottom, "a", "001.txt"))
	os.Create(filepath.Join(bottom, "a", "002.txt"))
	os.Create(filepath.Join(bottom, "a", "003.txt"))

	os.Mkdir(filepath.Join(bottom, "a", "b"), 0766)
	os.Create(filepath.Join(bottom, "a", "b", "010.txt"))
	os.Create(filepath.Join(bottom, "a", "b", "020.txt"))
	os.Create(filepath.Join(bottom, "a", "b", "030.txt"))
	os.Mkdir(filepath.Join(bottom, "a", "b", "c"), 0766)

	os.MkdirAll(filepath.Join(top, "a", "b"), 0766)
	// this should delete everything in the $bottom/a/b directory, but not the directory itself
	os.Create(filepath.Join(top, "a", "b", ".wh..wh..opq"))
	// this should delete the file $bottom/a/001.txt
	os.Create(filepath.Join(top, "a", ".wh.001.txt"))
	if err != nil {
		// maybe in some filesystem this call can fail, better make sure it actually worked.
		t.Errorf("Error in creating opaque file in directory, %s", err)
	}

	// Here we actually apply one directory on top of the other
	err = ApplyDirectory(bottom, top)
	if err != nil {
		t.Errorf("Error in applying the top directory %s over the bottom one: %s Err: %s", top, bottom, err)
	}

	// here we check the behaviour of the .wh..wh..opq file
	cancelledByOpaqueWhiteout := filepath.Join(bottom, "a", "b", "010.txt")
	if _, err := os.Stat(cancelledByOpaqueWhiteout); !os.IsNotExist(err) {
		t.Errorf("File %s was not cancelled by the opaque directory.", cancelledByOpaqueWhiteout)
	}

	cancelledByOpaqueWhiteout = filepath.Join(bottom, "a", "b", "020.txt")
	if _, err := os.Stat(cancelledByOpaqueWhiteout); !os.IsNotExist(err) {
		t.Errorf("File %s was not cancelled by the opaque directory.", cancelledByOpaqueWhiteout)
	}

	cancelledByOpaqueWhiteout = filepath.Join(bottom, "a", "b", "030.txt")
	if _, err := os.Stat(cancelledByOpaqueWhiteout); !os.IsNotExist(err) {
		t.Errorf("File %s was not cancelled by the opaque directory.", cancelledByOpaqueWhiteout)
	}

	cancelledByOpaqueWhiteout = filepath.Join(bottom, "a", "b", "c")
	if _, err := os.Stat(cancelledByOpaqueWhiteout); !os.IsNotExist(err) {
		t.Errorf("Directory %s was not cancelled by the opaque directory.", cancelledByOpaqueWhiteout)
	}

	notCancelledByOpaqueWhiteout := filepath.Join(bottom, "a", "b")
	if _, err := os.Stat(notCancelledByOpaqueWhiteout); os.IsNotExist(err) {
		t.Errorf("Direcotry %s was cancelled by the opaque directory, it should not.", cancelledByOpaqueWhiteout)
	}

	cancelledByWhiteout := filepath.Join(bottom, "a", "001.txt")
	if _, err := os.Stat(notCancelledByOpaqueWhiteout); !os.IsNotExist(err) {
		t.Errorf("File %s was not cancelled by the whiteout file in the top directory.", cancelledByWhiteout)
	}
}
