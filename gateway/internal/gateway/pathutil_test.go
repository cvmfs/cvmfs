package gateway

import "testing"

func TestPathOverlapCheck(t *testing.T) {
	t.Run("abc/def - abc", func(t *testing.T) {
		if !CheckPathOverlap("abc/def", "abc") {
			t.Fatal("paths are overlapping")
		}
	})
	t.Run("abc - abc/def", func(t *testing.T) {
		if !CheckPathOverlap("abc", "abc/def") {
			t.Fatal("paths are overlapping")
		}
	})
	t.Run("abcdef - abc", func(t *testing.T) {
		if CheckPathOverlap("abcdef", "abc") {
			t.Fatal("paths are not overlapping")
		}
	})
	t.Run("abc - abcdef", func(t *testing.T) {
		if CheckPathOverlap("abc", "abcdef") {
			t.Fatal("paths are not overlapping")
		}
	})
	t.Run("abc/def - abc/de", func(t *testing.T) {
		if CheckPathOverlap("abc/def", "abc/de") {
			t.Fatal("paths are not overlapping")
		}
	})
	t.Run("abc/de - abc/def", func(t *testing.T) {
		if CheckPathOverlap("abc/de", "abc/def") {
			t.Fatal("paths are not overlapping")
		}
	})
	t.Run("abc/def - ab", func(t *testing.T) {
		if CheckPathOverlap("abc/def", "ab") {
			t.Fatal("paths are not overlapping")
		}
	})
	t.Run("ab - abc/def", func(t *testing.T) {
		if CheckPathOverlap("ab", "abc/def") {
			t.Fatal("paths are not overlapping")
		}
	})
}

func TestSplitLeasePath(t *testing.T) {
	t.Run("test.repo.org/sub/path is ok", func(t *testing.T) {
		r, p, err := SplitLeasePath("test.repo.org/sub/path")
		if err != nil {
			t.Fatalf("valid input was rejected: %v", err)
		}
		if r != "test.repo.org" {
			t.Fatalf("invalid repository name extracted: %v", r)
		}
		if p != "/sub/path" {
			t.Fatalf("invalid subpath extracted: %v", p)
		}
	})
	t.Run("test.repo.org/ is ok", func(t *testing.T) {
		r, p, err := SplitLeasePath("test.repo.org/")
		if err != nil {
			t.Fatalf("valid input was rejected: %v", err)
		}
		if r != "test.repo.org" {
			t.Fatalf("invalid repository name extracted: %v", r)
		}
		if p != "/" {
			t.Fatalf("invalid subpath extracted: %v", p)
		}
	})
	t.Run("very.complex.fqdn.with.many.parts/sub/path is ok", func(t *testing.T) {
		r, p, err := SplitLeasePath("very.complex.fqdn.with.many.parts/sub/path")
		if err != nil {
			t.Fatalf("valid input was rejected: %v", err)
		}
		if r != "very.complex.fqdn.with.many.parts" {
			t.Fatalf("invalid repository name extracted: %v", r)
		}
		if p != "/sub/path" {
			t.Fatalf("invalid subpath extracted: %v", p)
		}
	})
	t.Run("/sub/path is invalid", func(t *testing.T) {
		if _, _, err := SplitLeasePath("/sub/path"); err == nil {
			t.Fatalf("invalid input was not rejected")
		}
	})
	t.Run("notfqdn/sub/path is invalid", func(t *testing.T) {
		if _, _, err := SplitLeasePath("notfqdn/sub/path"); err == nil {
			t.Fatalf("invalid input was not rejected")
		}
	})
	t.Run("notfqdn is invalid", func(t *testing.T) {
		if _, _, err := SplitLeasePath("notfqdn"); err == nil {
			t.Fatalf("invalid input was not rejected")
		}
	})
}
