package backend

import (
	"encoding/json"
	"fmt"
	"io"
	"os"

	gw "github.com/cvmfs/gateway/internal/gateway"

	"github.com/pkg/errors"
)

// AccessConfig is the configuration of a single repository
type AccessConfig struct {
	Repositories map[string]map[string]string
	Keys         map[string]string
}

// KeyIDToPath maintains the association of a key with a subpath inside the repository
type KeyIDToPath struct {
	ID   string `json:"id"`
	Path string `json:"path"`
}

// RepositorySpecV1 lists the keys associated with a repository in the configuration file
type RepositorySpecV1 struct {
	Name string   `json:"domain"`
	Keys []string `json:"keys"`
}

// RepositorySpecV2 lists the keys associated with a repository in the configuration file
type RepositorySpecV2 struct {
	Name string        `json:"domain"`
	Keys []KeyIDToPath `json:"keys"`
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

type rawConfig map[string]json.RawMessage

// NewAccessConfig creates an empty repository configuration
func NewAccessConfig() AccessConfig {
	return AccessConfig{
		Repositories: make(map[string]map[string]string),
		Keys:         make(map[string]string)}
}

// Load parses a configuration file and populates the repository
// configuration object
func (c *AccessConfig) Load(fileName string) error {
	return c.LoadWithImporter(fileName, keyImporter)
}

// LoadWithImporter parses a configuration file and populates the repository
// configuration object. Uses the specified key importer function
func (c *AccessConfig) LoadWithImporter(fileName string, importer KeyImportFun) error {
	f, err := os.Open(fileName)
	if err != nil {
		return errors.Wrap(err, "could not open input file for reading")
	}
	defer f.Close()
	return c.loadFromReader(f, importer)
}

func (c *AccessConfig) loadFromReader(rd io.Reader, importer KeyImportFun) error {
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
			c.Keys[keyID] = secret
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
			keyIds := make(map[string]string)
			for _, k := range spec.Keys {
				keyIds[k] = keyPaths[k]
			}
			c.Repositories[spec.Name] = keyIds
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
				c.Repositories[name] = map[string]string{"default": "default"}
			} else {
				// Item is a RepositorySpecV2; associate the key IDs and paths to the
				// repository
				ks := make(map[string]string)
				for _, k := range spec.Keys {
					ks[k.ID] = k.Path
				}
				c.Repositories[spec.Name] = ks
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
			c.Keys[keyID] = secret
		}
	}

	// Iterate over the repositories and replace "default" keys with actual key IDs
	// and secrets
	for repoName, ks := range c.Repositories {
		if _, present := ks["default"]; present {
			delete(ks, "default")
			spec := KeySpec{KeyType: "file", FileName: "/etc/cvmfs/keys/" + repoName + ".gw"}
			keyID, secret, _, err := importer(spec)
			if err != nil {
				return errors.Wrap(
					err, fmt.Sprintf("could not import default key for repository: %v", repoName))
			}
			if _, present := c.Keys[keyID]; !present {
				c.Keys[keyID] = secret
			}
			ks[keyID] = "/"
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
