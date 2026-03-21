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

// TestArchAliasesAllCandidatesAreKnownNormalizedNames verifies that every
// candidate in archAliases matches a directory name that GetNameWithArch
// can produce (i.e. the base component of the .multiarch/<dir> path).
func TestArchAliasesAllCandidatesAreKnownNormalizedNames(t *testing.T) {
	// Build the set of normalized arch dir names that GetNameWithArch can emit.
	type archVariant struct{ arch, variant string }
	cases := []archVariant{
		{"arm64", ""},
		{"arm64", "v8"},
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

	for alias, candidates := range archAliases {
		for _, candidate := range candidates {
			if _, ok := validDirs[candidate]; !ok {
				t.Errorf("archAliases[%q] candidate %q is not a known .multiarch dir name", alias, candidate)
			}
		}
	}
}

// TestArchAliasesNoSelfReferential checks that no alias lists itself as a
// candidate target, which would produce a symlink pointing to itself.
func TestArchAliasesNoSelfReferential(t *testing.T) {
	for alias, candidates := range archAliases {
		for _, candidate := range candidates {
			if alias == candidate {
				t.Errorf("archAliases[%q] includes itself as a candidate — would create a self-referential symlink", alias)
			}
		}
	}
}

// TestGetNameWithArchForAliasedArches verifies that the non-normalized arch
// names in archAliases do NOT appear as Platform.Architecture values that
// GetNameWithArch would normalise on its own (they come from the registry
// verbatim, so a separate alias symlink is required).
func TestGetNameWithArchForAliasedArches(t *testing.T) {
	for alias, candidates := range archAliases {
		entry := da.ManifestListItem{}
		entry.Platform.Architecture = alias
		got := GetNameWithArch(entry)
		for _, candidate := range candidates {
			wantNot := filepath.Join(".multiarch", candidate)
			if got == wantNot {
				t.Errorf("GetNameWithArch with arch=%q already returns %q; alias symlink would be redundant", alias, wantNot)
			}
		}
	}
}

// TestAarch64FallsBackToArm64v8 verifies that when only .multiarch/arm64:v8
// exists (and not plain .multiarch/arm64), the aarch64 alias still resolves
// correctly — i.e. arm64:v8 is listed as a fallback candidate for aarch64.
func TestAarch64FallsBackToArm64v8(t *testing.T) {
	candidates, ok := archAliases["aarch64"]
	if !ok {
		t.Fatal("aarch64 not found in archAliases")
	}
	hasArm64 := false
	hasArm64v8 := false
	for _, c := range candidates {
		if c == "arm64" {
			hasArm64 = true
		}
		if c == "arm64:v8" {
			hasArm64v8 = true
		}
	}
	if !hasArm64 {
		t.Error("aarch64 candidates must include \"arm64\"")
	}
	if !hasArm64v8 {
		t.Error("aarch64 candidates must include \"arm64:v8\" as fallback")
	}
	// arm64 must be listed before arm64:v8 so that plain arm64 is preferred.
	arm64Idx, arm64v8Idx := -1, -1
	for i, c := range candidates {
		switch c {
		case "arm64":
			arm64Idx = i
		case "arm64:v8":
			arm64v8Idx = i
		}
	}
	if arm64Idx > arm64v8Idx {
		t.Errorf("aarch64 candidates: \"arm64\" (idx %d) must come before \"arm64:v8\" (idx %d)", arm64Idx, arm64v8Idx)
	}
}

// TestArm64AliasTargetsArm64v8 verifies that "arm64" (without variant) is
// itself an alias whose first — and only — candidate is "arm64:v8".
func TestArm64AliasTargetsArm64v8(t *testing.T) {
	candidates, ok := archAliases["arm64"]
	if !ok {
		t.Fatal("arm64 not found in archAliases")
	}
	if len(candidates) != 1 || candidates[0] != "arm64:v8" {
		t.Errorf("archAliases[\"arm64\"] = %v; want [\"arm64:v8\"]", candidates)
	}
}
