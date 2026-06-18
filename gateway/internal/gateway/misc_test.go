package gateway

import (
	"encoding/json"
	"testing"
)

func TestRepositoryTagJSONRoundTrip(t *testing.T) {
	tag := RepositoryTag{
		Name:             "generic-2024-01-01T00:00:00Z",
		Description:      "auto tag",
		AutoTagThreshold: 1700000000,
	}

	data, err := json.Marshal(tag)
	if err != nil {
		t.Fatalf("failed to marshal RepositoryTag: %v", err)
	}

	var decoded RepositoryTag
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("failed to unmarshal RepositoryTag: %v", err)
	}

	if decoded.Name != tag.Name {
		t.Errorf("Name mismatch: got %q, want %q", decoded.Name, tag.Name)
	}
	if decoded.Description != tag.Description {
		t.Errorf("Description mismatch: got %q, want %q", decoded.Description, tag.Description)
	}
	if decoded.AutoTagThreshold != tag.AutoTagThreshold {
		t.Errorf("AutoTagThreshold mismatch: got %d, want %d", decoded.AutoTagThreshold, tag.AutoTagThreshold)
	}
}

func TestRepositoryTagJSONOmitEmptyThreshold(t *testing.T) {
	tag := RepositoryTag{
		Name:        "tag1",
		Description: "a tag",
	}

	data, err := json.Marshal(tag)
	if err != nil {
		t.Fatalf("failed to marshal RepositoryTag: %v", err)
	}

	// auto_tag_threshold should be omitted when zero
	var raw map[string]interface{}
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatalf("failed to unmarshal to map: %v", err)
	}
	if _, exists := raw["auto_tag_threshold"]; exists {
		t.Errorf("auto_tag_threshold should be omitted when zero, but was present")
	}
}

func TestRepositoryTagJSONDeleteTags(t *testing.T) {
	tag := RepositoryTag{
		DeleteTags: "tag1 tag2",
	}

	data, err := json.Marshal(tag)
	if err != nil {
		t.Fatalf("failed to marshal RepositoryTag: %v", err)
	}

	var decoded RepositoryTag
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("failed to unmarshal RepositoryTag: %v", err)
	}
	if decoded.DeleteTags != tag.DeleteTags {
		t.Errorf("DeleteTags mismatch: got %q, want %q", decoded.DeleteTags, tag.DeleteTags)
	}
}

func TestRepositoryTagJSONOmitEmptyDeleteTags(t *testing.T) {
	tag := RepositoryTag{
		Name:        "tag1",
		Description: "a tag",
	}

	data, err := json.Marshal(tag)
	if err != nil {
		t.Fatalf("failed to marshal RepositoryTag: %v", err)
	}

	// delete_tags should be omitted when empty so older gateways/receivers are
	// unaffected by regular (non-removal) commits
	var raw map[string]interface{}
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatalf("failed to unmarshal to map: %v", err)
	}
	if _, exists := raw["delete_tags"]; exists {
		t.Errorf("delete_tags should be omitted when empty, but was present")
	}
}

func TestRepositoryTagJSONBackwardsCompatible(t *testing.T) {
	// Simulate a message from an older publisher that doesn't send
	// auto_tag_threshold
	jsonStr := `{"tag_name":"tag1","tag_description":"desc"}`

	var tag RepositoryTag
	if err := json.Unmarshal([]byte(jsonStr), &tag); err != nil {
		t.Fatalf("failed to unmarshal old-format RepositoryTag: %v", err)
	}

	if tag.Name != "tag1" {
		t.Errorf("Name mismatch: got %q, want %q", tag.Name, "tag1")
	}
	if tag.Description != "desc" {
		t.Errorf("Description mismatch: got %q, want %q", tag.Description, "desc")
	}
	if tag.AutoTagThreshold != 0 {
		t.Errorf("AutoTagThreshold should be zero for old-format messages, got %d", tag.AutoTagThreshold)
	}
}
