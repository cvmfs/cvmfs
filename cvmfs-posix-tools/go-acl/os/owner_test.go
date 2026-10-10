// Copyright (c) 2015 Joseph Naegele. See LICENSE file.

package os

import (
	"io/ioutil"
	"os"
	"testing"
)

func TestOwner(t *testing.T) {
	uid := os.Getuid()
	gid := os.Getgid()

	f, err := ioutil.TempFile("./", "")
	if err != nil {
		t.Error(err)
	}
	name := f.Name()

	owner, group, err := Owner(name)
	if err != nil {
		t.Error(err)
	}

	if owner != uid {
		t.Fail()
	}
	if group != gid {
		t.Fail()
	}

	if err := f.Close(); err != nil {
		t.Error(err)
	}
	if err != os.Remove(name) {
		t.Error(err)
	}
}
