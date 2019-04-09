package gateway

const accessConfigV1 = `
{
	"repos": [
		{
			"domain": "test.cern.ch",
			"keys": ["<KEY_ID>"]
		}
	],
	"keys": [
		{
			"type": "file",
			"file_name": "/etc/cvmfs/keys/test.cern.ch.gw",
			"repo_subpath": "/"
		}
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
			"secret": "<SECRET>"
		}
	]
}
`
