package lib

import (
	"path/filepath"
	"testing"

	da "github.com/cvmfs/ducc/docker-api"
)

func TestOutputImageForExpandedTagWildcard(t *testing.T) {
	input := &Image{TagWildcard: true}
	output := &Image{Registry: "localhost:5000", Repository: "mock/repo", Tag: "*"}

	got := outputImageForExpandedTag(input, output, "v1")

	if got.Tag != "v1" {
		t.Fatalf("expected resolved tag v1, got %q", got.Tag)
	}
	if got.Registry != output.Registry || got.Repository != output.Repository {
		t.Fatalf("output image fields changed unexpectedly: got %s/%s", got.Registry, got.Repository)
	}
}

func TestOutputImageForExpandedTagFixedTag(t *testing.T) {
	input := &Image{TagWildcard: false}
	output := &Image{Registry: "localhost:5000", Repository: "mock/repo", Tag: "stable"}

	got := outputImageForExpandedTag(input, output, "ignored")

	if got.Tag != "stable" {
		t.Fatalf("expected fixed tag stable, got %q", got.Tag)
	}
}

func TestOutputRepositoryForImport(t *testing.T) {
	output := Image{Registry: "localhost:5000", Repository: "mock/repo", Tag: "stable"}

	got := outputRepositoryForImport(output)

	if got != "localhost:5000/mock/repo" {
		t.Fatalf("expected repository-only reference, got %q", got)
	}
}

func TestConversionSummaryPrefersAddedOverOtherStates(t *testing.T) {
	summary := ConversionSummary{}
	summary.Add(ConversionMatch, "example:latest")
	summary.Add(ConversionNotMatch, "example:latest")
	summary.Add(ConversionNotFound, "example:latest")

	if len(summary.Added) != 1 || summary.Added[0] != "example:latest" {
		t.Fatalf("expected image to be tracked as added, got %+v", summary)
	}
	if len(summary.Updated) != 0 {
		t.Fatalf("expected no updated images, got %+v", summary.Updated)
	}
	if len(summary.AlreadyConverted) != 0 {
		t.Fatalf("expected no already-converted images, got %+v", summary.AlreadyConverted)
	}
}

func TestConversionSummaryMergeKeepsHighestPriorityState(t *testing.T) {
	summary := ConversionSummary{AlreadyConverted: []string{"img-a", "img-b"}}
	summary.Merge(ConversionSummary{Updated: []string{"img-a"}, Added: []string{"img-b"}})

	if len(summary.Added) != 1 || summary.Added[0] != "img-b" {
		t.Fatalf("expected img-b to be added, got %+v", summary)
	}
	if len(summary.Updated) != 1 || summary.Updated[0] != "img-a" {
		t.Fatalf("expected img-a to be updated, got %+v", summary)
	}
	if len(summary.AlreadyConverted) != 0 {
		t.Fatalf("expected already-converted images to be cleared, got %+v", summary.AlreadyConverted)
	}
}

func TestImageNameWithPlatform(t *testing.T) {
	variant := "v8"
	image := &Image{Registry: "registry.example.org", Repository: "team/demo", Tag: "latest"}
	manifestEntry := da.ManifestListItem{}
	manifestEntry.Platform.OS = "linux"
	manifestEntry.Platform.Architecture = "arm64"
	manifestEntry.Platform.Variant = &variant

	got := imageNameWithPlatform(image, manifestEntry)
	want := "registry.example.org/team/demo:latest (linux/arm64/v8)"
	if got != want {
		t.Fatalf("expected %q, got %q", want, got)
	}
}

// TestArchAliasesAllPointToKnownNormalizedNames verifies that every value in
// archAliases matches the directory name that GetNameWithArch would produce
// (i.e. the base component of the .multiarch/<dir> path).
func TestArchAliasesAllPointToKnownNormalizedNames(t *testing.T) {
	// Build the set of normalized arch dir names that GetNameWithArch can emit.
	type archVariant struct{ arch, variant string }
	cases := []archVariant{
		{"arm64", ""},
		{"arm", ""},
		{"arm", "v6"},
		{"386", ""},
		{"amd64", ""},
	}
	validDirs := map[string]struct{}{}
	for _, c := range cases {
		entry := da.ManifestListItem{}
		entry.Platform.Architecture = c.arch
		if c.variant != "" {
			entry.Platform.Variant = &c.variant
		}
		nameWithArch := GetNameWithArch(entry)
		validDirs[filepath.Base(nameWithArch)] = struct{}{}
	}

	for alias, normalized := range archAliases {
		if _, ok := validDirs[normalized]; !ok {
			t.Errorf("archAliases[%q] = %q is not a known .multiarch dir name", alias, normalized)
		}
	}
}

// TestArchAliasesNonNormalizedNamesAreDistinct checks that no alias key is
// itself a normalized arch name (that would create a self-referential entry).
func TestArchAliasesNonNormalizedNamesAreDistinct(t *testing.T) {
	normalizedSet := map[string]struct{}{}
	for _, v := range archAliases {
		normalizedSet[v] = struct{}{}
	}
	for alias := range archAliases {
		if _, ok := normalizedSet[alias]; ok {
			t.Errorf("archAliases key %q is also a normalized arch name — would create a self-referential symlink", alias)
		}
	}
}

// TestGetNameWithArchForAliasedArches verifies that the non-normalized arch
// names in archAliases do NOT appear as Platform.Architecture values that
// GetNameWithArch would normalise on its own (they come from the registry
// verbatim, so a separate alias symlink is required).
func TestGetNameWithArchForAliasedArches(t *testing.T) {
	for alias, normalized := range archAliases {
		entry := da.ManifestListItem{}
		entry.Platform.Architecture = alias
		got := GetNameWithArch(entry)
		wantNot := filepath.Join(".multiarch", normalized)
		if got == wantNot {
			t.Errorf("GetNameWithArch with arch=%q already returns %q; alias symlink would be redundant", alias, wantNot)
		}
	}
}
