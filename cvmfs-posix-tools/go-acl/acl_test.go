// Copyright (c) 2015 Joseph Naegele. See LICENSE file.

package acl

import (
	"os"
	"testing"
)

const (
	tmpfile = "acl-tmp-test-file"
	tmpdir  = "acl-tmp-test-dir"
)

func getACLFromTmpFile(t *testing.T) *ACL {
	f, err := os.Create(tmpfile)
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	defer os.Remove(tmpfile)

	acl, err := GetFileAccess(tmpfile)
	if err != nil {
		t.Fatal("Failed to get ACL from file: ", err)
	}
	return acl
}

func checkForBaseEntries(t *testing.T, acl *ACL) {
	// the base access entries on a normal file should have
	// a USER_OBJ, GROUP_OBJ, and OTHER entry
	var u, g, o bool
	for e := acl.FirstEntry(); e != nil; e = acl.NextEntry() {
		tag, err := e.GetTag()
		if err != nil {
			t.Fatal(err)
		}
		switch tag {
		case TagUserObj:
			u = true
		case TagGroupObj:
			g = true
		case TagOther:
			o = true
		}
	}
	if !u || !g || !o {
		t.Fail()
	}
}

func TestGetFile(t *testing.T) {
	acl := getACLFromTmpFile(t)
	acl.Free()
}

func TestSetFile(t *testing.T) {
	acl := New()
	if acl == nil {
		t.Fatal("unable to create new ACL")
	}

	f, err := os.Create(tmpfile)
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	defer os.Remove(tmpfile)

	if err := acl.SetFileAccess(tmpfile); err != nil {
		t.Fatal(err)
	}
}

func TestGetEntries(t *testing.T) {
	acl := getACLFromTmpFile(t)
	defer acl.Free()
	checkForBaseEntries(t, acl)
}

func TestAddEntry(t *testing.T) {
	f, err := os.Create(tmpfile)
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	defer os.Remove(tmpfile)

	acl, err := GetFileAccess(tmpfile)
	if err != nil {
		t.Fatal("Failed to get ACL from file: ", err)
	}
	defer acl.Free()

	empty := New()
	if empty == nil {
		t.Fatal("unable to create new ACL")
	}
	for e := acl.FirstEntry(); e != nil; e = acl.NextEntry() {
		if err := empty.AddEntry(e); err != nil {
			t.Fatal(err)
		}
	}

	checkForBaseEntries(t, empty)
}
