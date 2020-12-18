package filesystem

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
	defer os.RemoveAll(bottom)
	err = os.Chmod(bottom, 0766)
	if err != nil {
		t.Errorf("Error in making the bottom directory open to write: %s", bottom)
	}

	top, err := ioutil.TempDir("", "top")
	if err != nil {
		t.Errorf("Error in creating the top directory: %s", top)
	}
	defer os.RemoveAll(top)

	// Create bottom directory
	os.Mkdir(filepath.Join(bottom, "a"), 0766)
	os.Create(filepath.Join(bottom, "a", "001.txt"))
	os.Create(filepath.Join(bottom, "a", "002.txt"))
	os.Create(filepath.Join(bottom, "a", "003.txt"))
	os.Mkdir(filepath.Join(bottom, "a", "zzz"), 0766)
	os.Mkdir(filepath.Join(bottom, "a", "xxx"), 0766)
	os.Create(filepath.Join(bottom, "a", "xxx", "100.txt"))
	os.Mkdir(filepath.Join(bottom, "a", "xxx", "yyy"), 0766)

	os.Mkdir(filepath.Join(bottom, "a", "b"), 0766)
	os.Create(filepath.Join(bottom, "a", "b", "010.txt"))
	os.Create(filepath.Join(bottom, "a", "b", "020.txt"))
	os.Create(filepath.Join(bottom, "a", "b", "030.txt"))
	os.Mkdir(filepath.Join(bottom, "a", "b", "c"), 0766)

	// test for a absolute symlink
	os.Mkdir(filepath.Join(bottom, "to_abs_symlink"), 0766)
	os.Mkdir(filepath.Join(bottom, "to_abs_symlink", "a"), 0766)
	os.Create(filepath.Join(bottom, "to_abs_symlink", "100.txt"))

	// test for a relative symlink
	os.Mkdir(filepath.Join(bottom, "to_rel_symlink"), 0766)
	os.Mkdir(filepath.Join(bottom, "to_rel_symlink", "a"), 0766)
	os.Create(filepath.Join(bottom, "to_rel_symlink", "100.txt"))

	// Create top directory to apply
	os.MkdirAll(filepath.Join(top, "a", "b"), 0766)
	// this should delete everything in the $bottom/a/b directory, but not the directory itself
	os.Create(filepath.Join(top, "a", "b", ".wh..wh..opq"))
	// this should delete the file $bottom/a/001.txt
	os.Create(filepath.Join(top, "a", ".wh.001.txt"))
	// this should delete the directory $bottom/a/zzz
	os.Create(filepath.Join(top, "a", ".wh.zzz"))
	// this should delete the directory $bottom/a/xxx and all the content
	os.Create(filepath.Join(top, "a", ".wh.xxx"))

	// move the to_abs_symlink directory to a symlink against something else
	os.MkdirAll(filepath.Join(top, "something_else_abs"), 0766)
	os.Symlink(filepath.Join(top, "something_else_abs"), filepath.Join(top, "to_abs_symlink"))
	os.MkdirAll(filepath.Join(top, "something_else_rel"), 0766)
	os.Symlink(filepath.Join(".", "something_else_rel"), filepath.Join(top, "to_rel_symlink"))

	// this should update the file $bottom/a/002.txt
	f002, _ := os.Create(filepath.Join(top, "a", "002.txt"))
	f002.WriteString("MiaoMiao")
	f002.Sync()
	f002.Close()

	// this should create the file $bottom/a/004.txt
	f004, _ := os.Create(filepath.Join(top, "a", "004.txt"))
	f004.WriteString("Simo❤Ema")
	f004.Sync()
	f004.Close()

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
	if _, err := os.Stat(cancelledByWhiteout); !os.IsNotExist(err) {
		t.Errorf("File %s was not cancelled by the whiteout file in the top directory.", cancelledByWhiteout)
	}

	cancelledByWhiteout = filepath.Join(bottom, "a", "zzz")
	if _, err := os.Stat(cancelledByWhiteout); !os.IsNotExist(err) {
		t.Errorf("Directory %s was not cancelled by the whiteout file in the top directory.", cancelledByWhiteout)
	}

	cancelledByWhiteout = filepath.Join(bottom, "a", "xxx", "100.txt")
	if _, err := os.Stat(cancelledByWhiteout); !os.IsNotExist(err) {
		t.Errorf("Directory %s was not cancelled by the whiteout file in the top directory.", cancelledByWhiteout)
	}

	cancelledByWhiteout = filepath.Join(bottom, "a", "xxx", "yyy")
	if _, err := os.Stat(cancelledByWhiteout); !os.IsNotExist(err) {
		t.Errorf("Directory %s was not cancelled by the whiteout file in the top directory.", cancelledByWhiteout)
	}

	cancelledByWhiteout = filepath.Join(bottom, "a", "xxx")
	if _, err := os.Stat(cancelledByWhiteout); !os.IsNotExist(err) {
		t.Errorf("Directory %s was not cancelled by the whiteout file in the top directory.", cancelledByWhiteout)
	}

	updated := filepath.Join(bottom, "a", "002.txt")
	if _, err := os.Stat(updated); os.IsNotExist(err) {
		t.Errorf("File %s does not exists but it should.", updated)
	}
	miaomiao, _ := ioutil.ReadFile(updated)
	if string(miaomiao) != "MiaoMiao" {
		t.Errorf("Content of %s is not expected '%s' vs '%s'", updated, string(miaomiao), "MiaoMiao")
	}

	notRemoved := filepath.Join(bottom, "a", "003.txt")
	if _, err := os.Stat(notRemoved); os.IsNotExist(err) {
		t.Errorf("Cannot find file %s but it should exists.", notRemoved)
	}

	created := filepath.Join(bottom, "a", "004.txt")
	if _, err := os.Stat(created); os.IsNotExist(err) {
		t.Errorf("File %s does not exists but it should.", created)
	}
	simo_ema, _ := ioutil.ReadFile(created)
	if string(simo_ema) != "Simo❤Ema" {
		t.Errorf("Content of %s is not expected '%s' vs '%s'", created, string(simo_ema), "Simo❤Ema")
	}

	linkPathAbs := filepath.Join(bottom, "to_abs_symlink")
	symlinkAbs, err := os.Stat(linkPathAbs)
	if err != nil {
		t.Errorf("Error in stating the new symlink")
	}
	if symlinkAbs.Mode()&os.ModeSymlink != 0 {
		t.Errorf("The symlink is not really a symlink")
	}
	AbsLinkPath, err := os.Readlink(linkPathAbs)
	if err != nil {
		t.Errorf("We cannot read the symlink")
	}
	if !filepath.IsAbs(AbsLinkPath) {
		t.Errorf("Absolute symlink is not an absolute path")
	}

	linkPathRel := filepath.Join(bottom, "to_rel_symlink")
	symlinkRel, err := os.Stat(linkPathRel)
	if err != nil {
		t.Errorf("Error in stating the new symlink")
	}
	if symlinkRel.Mode()&os.ModeSymlink != 0 {
		t.Errorf("The symlink is not really a symlink")
	}
	RelLinkPath, err := os.Readlink(linkPathRel)
	if err != nil {
		t.Errorf("We cannot read the symlink")
	}
	if filepath.IsAbs(RelLinkPath) {
		t.Errorf("Relative symlink is not a relative path")
	}

}
