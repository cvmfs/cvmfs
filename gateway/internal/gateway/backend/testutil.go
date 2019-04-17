package backend

import (
	"fmt"
	"time"

	gw "github.com/cvmfs/gateway/internal/gateway"
)

const accessConfigV1 = `
{
	"repos": [
		{
			"domain": "test.repo.org",
			"keys": ["keyid123"]
		}
	],
	"keys": [
		{
			"type": "file",
			"file_name": "/etc/cvmfs/keys/test.repo.org.gw",
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
		"test1.repo.org",
		{
			"domain": "test2.repo.org",
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
			"file_name": "/etc/cvmfs/keys/test2.repo.org.gw"
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
		"test1.repo.org"
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

func testConfig(workDir string) *gw.Config {
	return &gw.Config{
		Port:          4929,
		MaxLeaseTime:  50 * time.Millisecond, // use 50ms leases by default in testing mode
		UseEtcd:       false,
		LogLevel:      "info",
		LogTimestamps: false,
		NumReceivers:  1,
		ReceiverPath:  "/usr/bin/cvmfs_receiver",
		WorkDir:       workDir,
	}
}
