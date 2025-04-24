package lib

import (
	"testing"
)

func TestParseTags(t *testing.T) {
	imageString := "https://registry.hub.docker.com/library/redis:*"
	image, err := ParseImage(imageString)
	if err != nil {
		t.Errorf("Error in parsing %s", imageString)
	}
	if image.Scheme != "https" {
		t.Errorf("Image string parsing error: wrong scheme: %v", image.Scheme)
	}
	if image.Registry != "registry.hub.docker.com" {
		t.Errorf("Image string parsing error: wrong registry: %v", image.Registry)
	}
	if image.Repository != "library/redis" {
		t.Errorf("Image string parsing error: wrong repository: %v", image.Repository)
	}
	if image.Tag != "*" {
		t.Errorf("Image string parsing error: wrong tag: %v", image.Tag)
	}
	if !image.TagWildcard {
		t.Errorf("Image string parsing error: no wildcard was parsed")
	}
}

func TestParseTagsWithGlob(t *testing.T) {
	imageString := "https://registry.hub.docker.com/vernemq/vernemq:1.9.2*"
	// 1.9.2-1
	// 1.9.2-1-alpine
	// 1.9.2
	// 1.9.2-alpine

	image, err := ParseImage(imageString)
	if err != nil {
		t.Errorf("Error in parsing %s", imageString)
	}

	if image.Scheme != "https" {
		t.Errorf("Image string parsing error: wrong scheme: %v", image.Scheme)
	}
	if image.Registry != "registry.hub.docker.com" {
		t.Errorf("Image string parsing error: wrong registry: %v", image.Registry)
	}
	if image.Repository != "vernemq/vernemq" {
		t.Errorf("Image string parsing error: %v", image.Repository)
	}
	if image.Tag != "1.9.2*" {
		t.Errorf("Image string parsing error: wrong tag: %v", image.Tag)
	}
}

func TestFilterUsingGlobStarMatchEverything(t *testing.T) {
	input := []string{"vnje", "nc.cnrje", "5230.25.83"}
	result, err := filterUsingGlob("*", input)
	if err != nil {
		t.Errorf("Error in filtering: %s", err)
	}
	if len(result) != len(input) {
		t.Errorf("The match is missing something, different lengths %d != %d", len(result), len(input))
	}
	for i := range result {
		if result[i] != input[i] {
			t.Errorf("The match is missing something: %s != %s", result[i], input[i])
		}
	}
}

func TestFilterUsingGlobStar(t *testing.T) {
	input := []string{"aaaa", "aab.12-8", "2.3"}
	result, err := filterUsingGlob("a*", input)
	if err != nil {
		t.Errorf("Error in filtering: %s", err)
	}
	expected := []string{"aaaa", "aab.12-8"}
	if len(result) != len(expected) {
		t.Errorf("The match is missing something, different lengths %d != %d", len(result), len(input))
	}
	for i := range result {
		if result[i] != expected[i] {
			t.Errorf("The match is missing something: %s != %s", result[i], input[i])
		}
	}
}

func TestFilterUsingGlobAtBeginning(t *testing.T) {
	input := []string{"bar-foo", "ubuntu-foo", "foo-bar"}
	result, err := filterUsingGlob("*-foo", input)
	if err != nil {
		t.Errorf("Error in filtering: %s", err)
	}
	expected := []string{"bar-foo", "ubuntu-foo"}
	if len(result) != len(expected) {
		t.Errorf("The match is missing something, different lengths %d != %d", len(result), len(input))
	}
	for i := range result {
		if result[i] != expected[i] {
			t.Errorf("The match is missing something: %s != %s", result[i], input[i])
		}
	}
}

func TestFilterUsingGlobTwice(t *testing.T) {
	input := []string{"foo", "ubuntu-foo", "foo-bar", "version-foo-2", "nope"}
	result, err := filterUsingGlob("*foo*", input)
	if err != nil {
		t.Errorf("Error in filtering: %s", err)
	}
	expected := []string{"foo", "ubuntu-foo", "foo-bar", "version-foo-2"}
	if len(result) != len(expected) {
		t.Errorf("The match is missing something, different lengths %d != %d", len(result), len(input))
	}
	for i := range result {
		if result[i] != expected[i] {
			t.Errorf("The match is missing something: %s != %s", result[i], input[i])
		}
	}
}

func TestFilterUsingGlobRealLifeImages01(t *testing.T) {
	input := []string{"rhel6-m201911", "rhel6-m202001", "rhel6-m202002", "rhel6", "rhel7-m201911", "rhel7-m202001", "rhel7-m202002", "rhel7", "tmp-rhel6-m202002-20200213", "tmp-rhel7-m202002-20200213"}
	result, err := filterUsingGlob("rhel7-m*", input)
	if err != nil {
		t.Errorf("Error in filtering: %s", err)
	}
	expected := []string{"rhel7-m201911", "rhel7-m202001", "rhel7-m202002"}
	if len(result) != len(expected) {
		t.Errorf("The match is missing something, different lengths %d != %d", len(result), len(input))
	}
	for i := range result {
		if result[i] != expected[i] {
			t.Errorf("The match is missing something: %s != %s", result[i], input[i])
		}
	}
}

func TestFilterUsingGlobRealLifeImages02(t *testing.T) {
	input := []string{"rhel6-m201911", "rhel6-m202001", "rhel6-m202002", "rhel6", "rhel7-m201911", "rhel7-m202001", "rhel7-m202002", "rhel7", "tmp-rhel6-m202002-20200213", "tmp-rhel7-m202002-20200213"}
	result, err := filterUsingGlob("rhel7", input)
	if err != nil {
		t.Errorf("Error in filtering: %s", err)
	}
	expected := []string{"rhel7"}
	if len(result) != len(expected) {
		t.Errorf("The match is missing something, different lengths %d != %d", len(result), len(input))
	}
	for i := range result {
		if result[i] != expected[i] {
			t.Errorf("The match is missing something: %s != %s", result[i], input[i])
		}
	}
}
