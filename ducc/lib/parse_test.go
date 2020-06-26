package lib

import (
	"testing"
)

func TestParseImageSimple(t *testing.T) {
	imageString := "https://hub.docker.com/library/redis:5"
	image, err := ParseImage(imageString)
	if err != nil {
		t.Errorf("Error in parsing %s", imageString)
	}
	if image.Scheme != "https" {
		t.Errorf("Error in parse wrong scheme: %s", image.Scheme)
	}
	if image.Registry != "hub.docker.com" {
		t.Errorf("Error in parse wrong registry: %s", image.Registry)
	}
	if image.Repository != "library/redis" {
		t.Errorf("Error in parse wrong repository: %s", image.Repository)
	}
	if image.Tag != "5" {
		t.Errorf("Error in parse wrong tag: %s", image.Tag)
	}
	if image.TagWildcard {
		t.Errorf("Error in parse detect StarWildcard")
	}
}

func TestParseImageWithTagWildcard(t *testing.T) {
	imageString := "https://hub.docker.com/library/redis:*"
	image, err := ParseImage(imageString)
	if err != nil {
		t.Errorf("Error in parsing %s", imageString)
	}
	if image.Scheme != "https" {
		t.Errorf("Error in parse wrong scheme: %s", image.Scheme)
	}
	if image.Registry != "hub.docker.com" {
		t.Errorf("Error in parse wrong registry: %s", image.Registry)
	}
	if image.Repository != "library/redis" {
		t.Errorf("Error in parse wrong repository: %s", image.Repository)
	}
	if image.Tag != "*" {
		t.Errorf("Error in parse wrong tag: %s", image.Tag)
	}
	if !image.TagWildcard {
		t.Errorf("Error no TagWildcard detected")
	}
}

func TestParseImageNoTag(t *testing.T) {
	imageString := "https://hub.docker.com/library/redis/"
	image, err := ParseImage(imageString)
	if err != nil {
		t.Errorf("Error in parsing %s", imageString)
	}
	if image.Scheme != "https" {
		t.Errorf("Error in parse wrong scheme: %s", image.Scheme)
	}
	if image.Registry != "hub.docker.com" {
		t.Errorf("Error in parse wrong registry: %s", image.Registry)
	}
	if image.Repository != "library/redis" {
		t.Errorf("Error in parse wrong repository: %s", image.Repository)
	}
	if image.Tag != "" {
		t.Errorf("Error in parse wrong tag: %s", image.Tag)
	}
}

func TestParseImageTooManyColon(t *testing.T) {
	imageString := "https://hub.docker.com/library/redis:5:4"
	image, err := ParseImage(imageString)
	if err == nil {
		t.Errorf("Error, wrong string (%s), should return error", imageString)
	}

	emptyImage := Image{}
	if image != emptyImage {
		t.Error("Trying to return an image on a wrong string")
	}
}

func TestParseImageWithDigest(t *testing.T) {
	imageString := "https://hub.docker.com/library/redis@sha256:aaabbbccc"
	image, err := ParseImage(imageString)
	if err != nil {
		t.Errorf("Error in parsing %s", imageString)
	}
	if image.Scheme != "https" {
		t.Errorf("Error in parse wrong scheme: %s", image.Scheme)
	}
	if image.Registry != "hub.docker.com" {
		t.Errorf("Error in parse wrong registry: %s", image.Registry)
	}
	if image.Repository != "library/redis" {
		t.Errorf("Error in parse wrong repository: %s", image.Repository)
	}
	if image.Tag != "" {
		t.Errorf("Error in parse wrong tag: %s", image.Tag)
	}
	if image.Digest != "sha256:aaabbbccc" {
		t.Errorf("Error in parse wrong digest: %s", image.Digest)
	}
}

func TestParseImageWithTagAndDigest(t *testing.T) {
	imageString := "https://hub.docker.com/library/redis:5@sha256:aaabbbccc"
	image, err := ParseImage(imageString)
	if err != nil {
		t.Errorf("Error in parsing %s", imageString)
	}
	if image.Scheme != "https" {
		t.Errorf("Error in parse wrong scheme: %s", image.Scheme)
	}
	if image.Registry != "hub.docker.com" {
		t.Errorf("Error in parse wrong registry: %s", image.Registry)
	}
	if image.Repository != "library/redis" {
		t.Errorf("Error in parse wrong repository: %s", image.Repository)
	}
	if image.Tag != "5" {
		t.Errorf("Error in parse wrong tag: %s", image.Tag)
	}
	if image.Digest != "sha256:aaabbbccc" {
		t.Errorf("Error in parse wrong digest: %s", image.Digest)
	}
}

func TestParseImageWithoutProtocol(t *testing.T) {
	imageString := "hub.docker.com/library/redis:5"
	image, err := ParseImage(imageString)
	if err != nil {
		t.Errorf("We cannot manage images without protocol: %s", err)
	}
	// this call might panic if we are not able to manage the string
	image.GetReference()
}
