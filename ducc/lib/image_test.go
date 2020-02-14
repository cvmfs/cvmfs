package lib

import (
	"testing"
)

func TestGetTags(t *testing.T) {
	imageString := "https://registry.hub.docker.com/library/redis:*"
	image, err := ParseImage(imageString)
	if err != nil {
		t.Errorf("Error in parsing %s", imageString)
	}
	tagsExpanded, err := image.ExpandWildcard()
	if err != nil {
		t.Errorf("Error in expanding the tags %s", imageString)
	}
	if len(tagsExpanded) <= 1 {
		t.Errorf("Tag expansions didn't work, only %d tags", len(tagsExpanded))
	}
}

func TestGetTagsWithGlob(t *testing.T) {
	imageString := "https://registry.hub.docker.com/vernemq/vernemq:1.9.2*"
	// 1.9.2-1
	// 1.9.2-1-alpine
	// 1.9.2
	// 1.9.2-alpine

	image, err := ParseImage(imageString)
	if err != nil {
		t.Errorf("Error in parsing %s", imageString)
	}
	tagsExpanded, err := image.ExpandWildcard()
	if err != nil {
		t.Errorf("Error in expanding the tags %s", imageString)
	}
	// at the moment exists
	existingTags := []string{"1.9.2-1", "1.9.2-1-alpine", "1.9.2", "1.9.2-alpine"}
	tags := make(map[string]bool)
	for _, tag := range tagsExpanded {
		tags[tag.Tag] = true
	}

	for _, mustBeHereTag := range existingTags {
		if _, ok := tags[mustBeHereTag]; !ok {
			t.Errorf("Expected tag not found. Expected: %s", mustBeHereTag)
		}
	}
}

func TestFilterUsingGlobStarMatchEverything(t *testing.T) {
	garbage := []string{"vnje", "nc.cnrje", "5230.25.83"}
	result, err := filterUsingGlob("*", garbage)
	if err != nil {
		t.Errorf("Error in filtering: %s", err)
	}
	if len(result) != len(garbage) {
		t.Errorf("The match is missing something, different lenghts %d != %d", len(result), len(garbage))
	}
	for i, _ := range result {
		if result[i] != garbage[i] {
			t.Errorf("The match is missing something: %s != %s", result[i], garbage[i])
		}
	}
}

func TestFilterUsingGlobStar(t *testing.T) {
	garbage := []string{"aaaa", "aab.12-8", "2.3"}
	result, err := filterUsingGlob("a*", garbage)
	if err != nil {
		t.Errorf("Error in filtering: %s", err)
	}
	expected := []string{"aaaa", "aab.12-8"}
	if len(result) != len(expected) {
		t.Errorf("The match is missing something, different lenghts %d != %d", len(result), len(garbage))
	}
	for i, _ := range result {
		if result[i] != expected[i] {
			t.Errorf("The match is missing something: %s != %s", result[i], garbage[i])
		}
	}
}

func TestFilterUsingGlobAtBeginning(t *testing.T) {
	garbage := []string{"bar-foo", "ubuntu-foo", "foo-bar"}
	result, err := filterUsingGlob("*-foo", garbage)
	if err != nil {
		t.Errorf("Error in filtering: %s", err)
	}
	expected := []string{"bar-foo", "ubuntu-foo"}
	if len(result) != len(expected) {
		t.Errorf("The match is missing something, different lenghts %d != %d", len(result), len(garbage))
	}
	for i, _ := range result {
		if result[i] != expected[i] {
			t.Errorf("The match is missing something: %s != %s", result[i], garbage[i])
		}
	}
}

func TestFilterUsingGlobTwice(t *testing.T) {
	garbage := []string{"foo", "ubuntu-foo", "foo-bar", "version-foo-2", "nope"}
	result, err := filterUsingGlob("*foo*", garbage)
	if err != nil {
		t.Errorf("Error in filtering: %s", err)
	}
	expected := []string{"foo", "ubuntu-foo", "foo-bar", "version-foo-2"}
	if len(result) != len(expected) {
		t.Errorf("The match is missing something, different lenghts %d != %d", len(result), len(garbage))
	}
	for i, _ := range result {
		if result[i] != expected[i] {
			t.Errorf("The match is missing something: %s != %s", result[i], garbage[i])
		}
	}
}
