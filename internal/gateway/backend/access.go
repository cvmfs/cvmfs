package backend

import (
	"encoding/json"
	"fmt"
	"io"
	"os"

	gw "github.com/cvmfs/gateway/internal/gateway"

	"github.com/pkg/errors"
)

// KeySettings contains the repository subpath associated with a key and a boolean
// flag showing if the key can be used for administration operations
type KeySettings struct {
	Path  string `json:"path"`
	Admin bool   `json:"admin"`
}

// RepositoryConfig contains the access configuration (registered keys and
// enabled status) for a repository
type RepositoryConfig struct {
	Keys    map[string]KeySettings `json:"keys"`
	Enabled bool                   `json:"enabled"`
}

// KeyConfig contains the secret part and the enabled status of a key
type KeyConfig struct {
	Secret  string
	Enabled bool
}

// AccessConfig is the configuration of a single repository
type AccessConfig struct {
	Repositories map[string]RepositoryConfig
	Keys         map[string]KeyConfig
}

// RepositorySpecV1 lists the keys associated with a repository in the configuration file
type RepositorySpecV1 struct {
	Name string   `json:"domain"`
	Keys []string `json:"keys"`
}

// RepositorySpecV2 lists the keys associated with a repository in the configuration file
type RepositorySpecV2 struct {
	Name string `json:"domain"`
	Keys []struct {
		ID    string `json:"id"`
		Admin bool   `json:"admin"`
		Path  string `json:"path"`
	} `json:"keys"`
}

// KeySpec is a gateway key specification from the configuration file
type KeySpec struct {
	KeyType  string `json:"type"`         // plain_text || file
	ID       string `json:"id"`           // required for type "plain_text"
	Secret   string `json:"secret"`       // required for type "plain_text"
	FileName string `json:"file_name"`    // required for type "file"
	Path     string `json:"repo_subpath"` // present if config is v1
}

// KeyImportFun is the prototype of the function which imports keys based on
// their specification in the configuration file. Returns two strings
// (the key ID and secret) and an error
type KeyImportFun func(KeySpec) (string, string, string, error)

// AuthError is returned as error value by the Check function when
// there is an authorization error (invalid key, invalid path, or invalid repo)
type AuthError struct {
	Reason string
}

func (ae AuthError) Error() string {
	return fmt.Sprintf("authorization error: %v", ae.Reason)
}

type rawConfig map[string]json.RawMessage

// NewAccessConfig creates an new access configuration from the given file
func NewAccessConfig(fileName string) (*AccessConfig, error) {
	return newAccessConfigWithImporter(fileName, keyImporter)
}

// IsRepositoryEnabled returns true if the repository is enabled
func (c *AccessConfig) IsRepositoryEnabled(name string) bool {
	return c.Repositories[name].Enabled
}

// SetRepositoryEnabled sets the enabled status of a repository
func (c *AccessConfig) SetRepositoryEnabled(name string, enabled bool) {
	if cfg, present := c.Repositories[name]; present {
		cfg.Enabled = enabled
		c.Repositories[name] = cfg
	}
}

// IsKeyEnabled returns true if the repository is enabled
func (c *AccessConfig) IsKeyEnabled(keyID string) bool {
	return c.Keys[keyID].Enabled
}

// SetKeyEnabled sets the enabled status of a key
func (c *AccessConfig) SetKeyEnabled(keyID string, enabled bool) {
	if cfg, present := c.Keys[keyID]; present {
		cfg.Enabled = enabled
		c.Keys[keyID] = cfg
	}
}

// GetRepos returns a map where the keys are repository names and the
// values are KeyPaths maps
func (c *AccessConfig) GetRepos() map[string]RepositoryConfig {
	return c.Repositories
}

// GetRepo returns a map where the keys are key ID registered for the
// repository and the values are repository subpath where the keys are
// valid
func (c *AccessConfig) GetRepo(repoName string) RepositoryConfig {
	return c.Repositories[repoName]
}

// GetKeyConfig returns the key configuration corresponding to a key ID
func (c *AccessConfig) GetKeyConfig(keyID string) KeyConfig {
	return c.Keys[keyID]
}

// Check verifies the given key and path are compatible with the access
// configuration of the repository
func (c *AccessConfig) Check(keyID, leasePath, repoName string) *AuthError {
	cfg, ok := c.Repositories[repoName]
	if !ok {
		return &AuthError{"invalid_repo"}
	}

	keySettings, ok := cfg.Keys[keyID]
	if !ok {
		return &AuthError{"invalid_key"}
	}

	overlapping := gw.CheckPathOverlap(leasePath, keySettings.Path)
	isSubpath := len(leasePath) >= len(keySettings.Path)

	if !overlapping || !isSubpath {
		return &AuthError{"invalid_path"}
	}

	return nil
}

func newAccessConfigWithImporter(fileName string, importer KeyImportFun) (*AccessConfig, error) {
	ac := emptyAccessConfig()

	f, err := os.Open(fileName)
	if err != nil {
		return nil, errors.Wrap(err, "could not open input file for reading")
	}
	defer f.Close()

	if err := ac.load(f, importer); err != nil {
		return nil, err
	}

	return &ac, nil
}

func emptyAccessConfig() AccessConfig {
	return AccessConfig{
		Repositories: make(map[string]RepositoryConfig),
		Keys:         make(map[string]KeyConfig),
	}
}

// load populates the access config object from the provided reader, using the
// specified key import function
func (c *AccessConfig) load(rd io.Reader, importer KeyImportFun) error {
	var t rawConfig
	if err := json.NewDecoder(rd).Decode(&t); err != nil {
		return errors.Wrap(err, "could not decode JSON input")
	}

	version := getConfigVersion(t)

	if version == 1 {
		return c.loadV1(t, importer)
	}

	return c.loadV2(t, importer)
}

func (c *AccessConfig) loadV1(cfg rawConfig, importer KeyImportFun) error {
	keyPaths := make(map[string]string)

	// Load the keys from the config file and store the KeyID -> Secret mapping
	// Save the KeyID -> Subpath mapping for later
	if rawKeys, present := cfg["keys"]; present {
		keys := make([]KeySpec, 0)
		if err := json.Unmarshal(rawKeys, &keys); err != nil {
			return errors.Wrap(err, "could not parse key specs")
		}
		for _, spec := range keys {
			keyID, secret, repoPath, err := importer(spec)
			if err != nil {
				return errors.Wrap(err, fmt.Sprintf("could not import key %v", spec.ID))
			}
			keyPaths[keyID] = repoPath
			c.Keys[keyID] = KeyConfig{Secret: secret, Enabled: true}
		}
	}

	// Load the repository specs from the config file and store the
	// RepoID -> (KeyID -> Subpath) mapping.
	if rawRepos, present := cfg["repos"]; present {
		repos := make([]RepositorySpecV1, 0)
		if err := json.Unmarshal(rawRepos, &repos); err != nil {
			return errors.Wrap(err, "could not import repository specs")
		}
		for _, spec := range repos {
			keyIds := make(map[string]KeySettings)
			for _, k := range spec.Keys {
				keyIds[k] = KeySettings{Path: keyPaths[k], Admin: false}
			}
			c.Repositories[spec.Name] = RepositoryConfig{Keys: keyIds, Enabled: true}
		}
	}

	return nil
}

func (c *AccessConfig) loadV2(cfg rawConfig, importer KeyImportFun) error {
	if rawRepos, present := cfg["repos"]; present {
		// Load the repository specifications as a list of json.RawMessage
		rawList := make([]json.RawMessage, 0)
		if err := json.Unmarshal(rawRepos, &rawList); err != nil {
			return errors.Wrap(err, "could not import repository specs")
		}

		// For each entry of the list, determine if it is a full repository
		// specification (v2) or the name of a repository
		for _, r := range rawList {
			var spec RepositorySpecV2
			if err := json.Unmarshal(r, &spec); err != nil {
				var name string
				if err := json.Unmarshal(r, &name); err != nil {
					return errors.Wrap(err, "could not interpret repository spec item")
				}
				// Item is a string representing the repository name; default key
				// from /etc/cvmfs/keys/<REPO_NAME>/ will be associated
				c.Repositories[name] = RepositoryConfig{
					Keys:    map[string]KeySettings{"default": KeySettings{Path: "default"}},
					Enabled: true,
				}
			} else {
				// Item is a RepositorySpecV2; associate the key IDs and paths to the
				// repository
				ks := make(map[string]KeySettings)
				for _, k := range spec.Keys {
					ks[k.ID] = KeySettings{Path: k.Path, Admin: k.Admin}
				}
				c.Repositories[spec.Name] = RepositoryConfig{
					Keys:    ks,
					Enabled: true,
				}
			}
		}
	}

	// Load the keys from the config file and store the KeyID -> Secret mapping
	if rawKeys, present := cfg["keys"]; present {
		keys := make([]KeySpec, 0)
		if err := json.Unmarshal(rawKeys, &keys); err != nil {
			return errors.Wrap(err, "could not import key specs")
		}
		for _, spec := range keys {
			keyID, secret, _, err := importer(spec)
			if err != nil {
				return errors.Wrap(err, fmt.Sprintf("could not import key %v", spec.ID))
			}
			c.Keys[keyID] = KeyConfig{Secret: secret, Enabled: true}
		}
	}

	// Iterate over the repositories and replace "default" keys with actual key IDs
	// and secrets
	for repoName, rc := range c.Repositories {
		if _, present := rc.Keys["default"]; present {
			delete(rc.Keys, "default")
			spec := KeySpec{KeyType: "file", FileName: "/etc/cvmfs/keys/" + repoName + ".gw"}
			keyID, secret, _, err := importer(spec)
			if err != nil {
				return errors.Wrap(
					err, fmt.Sprintf("could not import default key for repository: %v", repoName))
			}
			if _, present := c.Keys[keyID]; !present {
				c.Keys[keyID] = KeyConfig{Secret: secret, Enabled: true}
			}
			rc.Keys[keyID] = KeySettings{Path: "/", Admin: false}
		}
	}

	return nil
}

func keyImporter(ks KeySpec) (string, string, string, error) {
	switch ks.KeyType {
	case "plain_text":
		return ks.ID, ks.Secret, ks.Path, nil
	case "file":
		id, sec, err := gw.LoadKey(ks.FileName)
		if err != nil {
			return "", "", "", fmt.Errorf("could not import key from file")
		}
		return id, sec, ks.Path, nil
	default:
		return "", "", "", fmt.Errorf("unknown key type")
	}
}

func getConfigVersion(cfg rawConfig) int {
	var version = 1 // default version, if omitted
	rawVer, present := cfg["version"]
	if present {
		v := 1
		if err := json.Unmarshal(rawVer, &v); err != nil {
			return version
		}
		version = v
	}
	return version
}
