package gateway

// RepositorySpec is the configuration of a single repository
type RepositorySpec struct {
	Name string        `json:"domain"`
	Keys []KeyIDToPath `json:"keys"`
}

// KeyIDToPath holds the public ID of a key and the path it is assigned to
type KeyIDToPath struct {
	ID   string
	Path string
}

// LoadRepositoryConfig parses a configuration file returning a repository
// configuration object
func LoadRepositoryConfig(fileName string) ([]RepositorySpec, error) {
	return nil, nil
}
