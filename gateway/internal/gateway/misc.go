package gateway

// RepositoryTag represents a tag of a CernVM-FS repository
type RepositoryTag struct {
	Name        string `json:"tag_name"`
	Description string `json:"tag_description"`
}
