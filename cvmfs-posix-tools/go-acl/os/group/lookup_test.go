// Copyright (c) 2015 Joseph Naegele. See LICENSE file.

package group

import (
	"runtime"
	"testing"
)

func check(t *testing.T) {
	if !implemented {
		t.Skip("group: not implemented; skipping tests")
	}
}

func TestCurrent(t *testing.T) {
	check(t)

	g, err := Current()
	if err != nil {
		t.Fatalf("Current: %v", err)
	}
	if g.Name == "" {
		t.Errorf("didn't get a groupname")
	}
}

func compare(t *testing.T, want, got *Group) {
	if want.Gid != got.Gid {
		t.Errorf("got Gid=%q; want %q", got.Gid, want.Gid)
	}
	if want.Name != got.Name {
		t.Errorf("got Name=%q; want %q", got.Name, want.Name)
	}
	if len(want.Members) != len(got.Members) {
		t.Errorf("got %d members; want %d", len(got.Members), len(want.Members))
	}
	for i, m := range want.Members {
		if m != got.Members[i] {
			t.Errorf("got Member=%q; want %q", got.Members[i], m)
		}
	}
}

func TestLookup(t *testing.T) {
	check(t)

	if runtime.GOOS == "plan9" {
		t.Skipf("Lookup not implemented on %q", runtime.GOOS)
	}

	want, err := Current()
	if err != nil {
		t.Fatalf("Current: %v", err)
	}
	got, err := Lookup(want.Name)
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	compare(t, want, got)
}

func TestLookupId(t *testing.T) {
	check(t)

	if runtime.GOOS == "plan9" {
		t.Skipf("LookupId not implemented on %q", runtime.GOOS)
	}

	want, err := Current()
	if err != nil {
		t.Fatalf("Current: %v", err)
	}
	got, err := LookupId(want.Gid)
	if err != nil {
		t.Fatalf("LookupId: %v", err)
	}
	compare(t, want, got)
}
