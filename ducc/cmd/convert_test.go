package cmd

import (
	"testing"
)

func TestIsInIgnoreList(t *testing.T) {
	tests := []struct {
		name       string
		imageName  string
		ignoreList []string
		expected   bool
	}{
		{
			name:       "exact match",
			imageName:  "https://registry.hub.docker.com/library/ubuntu:20.04",
			ignoreList: []string{"https://registry.hub.docker.com/library/ubuntu:20.04"},
			expected:   true,
		},
		{
			name:       "wildcard match - tag",
			imageName:  "https://registry.hub.docker.com/library/nginx:latest",
			ignoreList: []string{"https://registry.hub.docker.com/library/nginx:*"},
			expected:   true,
		},
		{
			name:       "wildcard match - repository",
			imageName:  "https://registry.hub.docker.com/library/alpine:latest",
			ignoreList: []string{"https://registry.hub.docker.com/library/*:latest"},
			expected:   true,
		},
		{
			name:       "no match",
			imageName:  "https://registry.hub.docker.com/library/ubuntu:22.04",
			ignoreList: []string{"https://registry.hub.docker.com/library/alpine:*"},
			expected:   false,
		},
		{
			name:       "empty ignore list",
			imageName:  "https://registry.hub.docker.com/library/ubuntu:20.04",
			ignoreList: []string{},
			expected:   false,
		},
		{
			name:       "multiple patterns - match second",
			imageName:  "https://registry.hub.docker.com/library/nginx:1.21",
			ignoreList: []string{"https://registry.hub.docker.com/library/alpine:*", "https://registry.hub.docker.com/library/nginx:*"},
			expected:   true,
		},
		{
			name:       "multiple patterns - no match",
			imageName:  "https://registry.hub.docker.com/library/ubuntu:20.04",
			ignoreList: []string{"https://registry.hub.docker.com/library/alpine:*", "https://registry.hub.docker.com/library/nginx:*"},
			expected:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isInIgnoreList(tt.imageName, tt.ignoreList)
			if result != tt.expected {
				t.Errorf("isInIgnoreList(%q, %v) = %v, expected %v", tt.imageName, tt.ignoreList, result, tt.expected)
			}
		})
	}
}

func TestSortedImageList(t *testing.T) {
	result := sortedImageList([]string{"z-image", "a-image", "m-image"})
	expected := []string{"a-image", "m-image", "z-image"}
	if len(result) != len(expected) {
		t.Fatalf("expected %v, got %v", expected, result)
	}
	for i, v := range expected {
		if result[i] != v {
			t.Fatalf("expected %v at index %d, got %v", v, i, result[i])
		}
	}
}

func TestSortedImageListEmpty(t *testing.T) {
	result := sortedImageList([]string{})
	if len(result) != 0 {
		t.Fatalf("expected empty slice, got %v", result)
	}
}

func TestSortedImageListDoesNotMutateInput(t *testing.T) {
	input := []string{"z-image", "a-image"}
	_ = sortedImageList(input)
	if input[0] != "z-image" {
		t.Fatalf("sortedImageList mutated the input slice")
	}
}
