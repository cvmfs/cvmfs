package lib

import "testing"

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

