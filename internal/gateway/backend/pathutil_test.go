package backend

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
