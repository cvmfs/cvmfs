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
