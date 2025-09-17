# cvmfs_rsync and cvmfs toolbox
A repository for the cvmfs_rsync project and all associated cvmfs convenience tools. The tools are listed here:

```
cvmfs_chgrp
cvmfs_chmod
cvmfs_chown
cvmfs_ln
cvmfs_mkdir
cvmfs_rm
cvmfs_rmdir
cvmfs_rsync
cvmfs_setfacl
```

These tools are meant to replicate the linux behavior for the cvmfs file system.

# Table of Contents
1. [Installation](#installation)
2. [Configuration Setup](#configuration-setup)
3. [Usage](#usage)
4. [Testing](#testing)
5. [Uninstall](#uninstall)

## Installation

Clone the Repo:
```
<REDACTED>
```

Change Directories:
```
cd cvmfs_rsync
```

Install the tools:
```
go install ./...
```

Note - You can install individual tools by specifiying the folder for the tool instead of `./...`, i.e.
```
go install ./cmd/cvmfs_chgrp
```
Note: You must have the `cvmfs-swissknife` tool installed to use this code.

### Package Troubleshooting

You should be able to install all auxiliary packages with
```
go mod tidy
```

That being said, the `bitbucket.org/dchapes/mode` package may have issues when being installed this way. To get around these, run the `go get` separately.
```
go get bitbucket.org/dchapes/mode
```

## Configuration Setup

The configuration for each repo should be located at `/etc/cvmfs/gateway-client/<repo-name>/cvmfs-rsync.yaml` and should have the format of the `.cvmfs_rsync_schema.yaml` file.

## Usage

Usage of these tools is exactly as one would expect from the linux tools. For more details, look at the readme at `cmd/<tool_name>` or run the tool with the `--help` flag.

## Testing

It is recommended to test each tool separately, to do so, read the testing section in the tool that you wish to test.

### Pre-requisites

1. `podman` and `cvmfs2` must be installed
2. `/var/run/cvmfs` and `/var/lib/cvmfs` need to be writeable by the user running the tests
3. The keys from `./pkg/etc/cvmfs/keys` need to be in `/etc/cvmfs/keys/test.repo.*`


