package backend

import (
	"fmt"
	"strings"
	"testing"
)

const accessConfigV1 = `
{
	"repos": [
		{
			"domain": "test.cern.ch",
			"keys": ["keyid123"]
		}
	],
	"keys": [
		{
			"type": "file",
			"file_name": "/etc/cvmfs/keys/test.cern.ch.gw",
			"repo_subpath": "/"
		},
		{
			"type": "plain_text",
			"id": "keyid2",
			"secret": "secret2",
			"repo_subpath": "/"
		}
	]
}
`
const accessConfigV2 = `
{
	"version": 2,
	"repos" : [
		"test1.cern.ch",
		{
			"domain": "test2.cern.ch",
			"keys": [
				{
					"id": "keyid1",
					"path": "/"
				},
				{
					"id": "keyid2",
					"path": "/restricted/to/subdir"
				}
			]
		}
	],
	"keys": [
		{
			"type": "file",
			"file_name": "/etc/cvmfs/keys/test2.cern.ch.gw"
		},
		{
			"type": "plain_text",
			"id": "keyid2",
			"secret": "secret2"
		}
	]
}
`

const accessConfigV2NoKeys = `
{
	"version": 2,
	"repos" : [
		"test1.cern.ch"
	]
}
`

func mockKeyImporter(ks KeySpec) (string, string, string, error) {
	switch ks.KeyType {
	case "plain_text":
		return ks.ID, ks.Secret, ks.Path, nil
	case "file":
		return "keyid123", "secret123", "/", nil
	default:
		return "", "", "", fmt.Errorf("unknown key type")
	}
}

func TestEmptyAccessConfig(t *testing.T) {
	t.Run("empty no version", func(t *testing.T) {
		ac := NewAccessConfig()
		rd := strings.NewReader("{}")
		err := ac.loadFromReader(rd, mockKeyImporter)
		if err != nil {
			t.Fatalf("access config loading failed: %v", err)
		}
	})
	t.Run("empty version 1", func(t *testing.T) {
		ac := NewAccessConfig()
		rd := strings.NewReader("{\"version\": 1}")
		err := ac.loadFromReader(rd, mockKeyImporter)
		if err != nil {
			t.Fatalf("access config loading failed: %v", err)
		}
	})
	t.Run("empty version 2", func(t *testing.T) {
		ac := NewAccessConfig()
		rd := strings.NewReader("{\"version\": 2}")
		err := ac.loadFromReader(rd, mockKeyImporter)
		if err != nil {
			t.Fatalf("access config loading failed: %v", err)
		}
	})
}

func TestLoadAccessConfigVersion1(t *testing.T) {
	ac := NewAccessConfig()
	rd := strings.NewReader(accessConfigV1)
	err := ac.loadFromReader(rd, mockKeyImporter)
	if err != nil {
		t.Fatalf("access config loading failed: %v", err)
	}
	if len(ac.Repositories) != 1 && len(ac.Keys) != 2 {
		t.Fatalf("invalid access config (missing items): %+v", ac)
	}
}

func TestLoadAccessConfigVersion2(t *testing.T) {
	ac := NewAccessConfig()
	rd := strings.NewReader(accessConfigV2)
	err := ac.loadFromReader(rd, mockKeyImporter)
	if err != nil {
		t.Fatalf("access config loading failed: %v", err)
	}
	if len(ac.Repositories) != 2 && len(ac.Keys) != 2 {
		t.Fatalf("invalid access config (missing items): %+v", ac)
	}
	if _, present := (ac.Repositories["test1.cern.ch"]["keyid123"]); !present {
		t.Fatalf("invalid access config (placeholder was not substituted): %+v", ac)
	}
	if _, present := ac.Keys["keyid123"]; !present {
		t.Fatalf("invalid access config (default key was not loaded): %+v", ac)
	}
}

func TestLoadAccessConfigVersion2NoKeys(t *testing.T) {
	ac := NewAccessConfig()
	rd := strings.NewReader(accessConfigV2NoKeys)
	err := ac.loadFromReader(rd, mockKeyImporter)
	if err != nil {
		t.Fatalf("access config loading failed: %v", err)
	}
	if len(ac.Repositories) != 1 && len(ac.Keys) != 1 {
		t.Fatalf("invalid access config (missing items): %+v", ac)
	}
	if _, present := (ac.Repositories["test1.cern.ch"]["keyid123"]); !present {
		t.Fatalf("invalid access config (placeholder was not substituted): %+v", ac)
	}
	if _, present := ac.Keys["keyid123"]; !present {
		t.Fatalf("invalid access config (default key was not loaded): %+v", ac)
	}
}
