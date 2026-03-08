package lib

import (
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
	if len(summary.Skipped) != 0 {
		t.Fatalf("expected no skipped images, got %+v", summary.Skipped)
	}
}

func TestConversionSummaryMergeKeepsHighestPriorityState(t *testing.T) {
	summary := ConversionSummary{Skipped: []string{"img-a", "img-b"}}
	summary.Merge(ConversionSummary{Updated: []string{"img-a"}, Added: []string{"img-b"}})

	if len(summary.Added) != 1 || summary.Added[0] != "img-b" {
		t.Fatalf("expected img-b to be added, got %+v", summary)
	}
	if len(summary.Updated) != 1 || summary.Updated[0] != "img-a" {
		t.Fatalf("expected img-a to be updated, got %+v", summary)
	}
	if len(summary.Skipped) != 0 {
		t.Fatalf("expected skipped images to be cleared, got %+v", summary.Skipped)
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
