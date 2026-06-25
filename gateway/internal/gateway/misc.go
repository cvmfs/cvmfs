package gateway

// RepositoryTag represents a tag of a CernVM-FS repository
type RepositoryTag struct {
	Name        string `json:"tag_name"`
	Description string `json:"tag_description"`
	// AutoTagThreshold is a Unix timestamp: auto-generated tags older than this
	// are removed by the receiver on commit. 0 (omitted) disables the cleanup.
	// The publisher resolves CVMFS_AUTO_TAG_TIMESPAN to an absolute timestamp
	// before sending it, so the gateway only ever forwards an integer.
	AutoTagThreshold int64 `json:"auto_tag_threshold,omitempty"`
	// DeleteTags is a space-separated list of existing tag names that the
	// receiver removes in the same history transaction as the commit. Empty
	// (omitted) disables removal. Used by `cvmfs_server tag -r` on gateway
	// repositories, where the publisher cannot edit the tag database directly.
	DeleteTags string `json:"delete_tags,omitempty"`
}
