# CVMFS POSIX tools

A repository for CVMFS convenience tools.

They work by submitting requests to CVMFS gateway and the S3 backend, and then wait for the changes, if any, to appear on the affected CVMFS mountpoint.
They do not invoke `cvmfs_server transaction` and `cvmfs_server publish`.
Instead, they use `cvmfs_swissknife ingestsql`.

The tools are:

```
cvmfs_chgrp
cvmfs_chmod
cvmfs_chown
cvmfs_insert
cvmfs_ln
cvmfs_mkdir
cvmfs_rm
cvmfs_rmdir
cvmfs_rsync
cvmfs_setfacl
cvmfs_touch
```

These tools are meant to replicate the behaviour of the respective common tools, for CVMFS.

# Table of Contents
1. [Installation](#installation)
2. [Configuration Setup](#configuration-setup)
3. [Usage](#usage)
4. [Testing](#testing)
5. [Uninstall](#uninstall)

## Installation

Enable CVMFS project's cmake option `BUILD_POSIX_TOOLS` to build and install this toolkit.
You need to have gateway component installed somewhere.
Gateway is enabled with `BUILD_GATEWAY` cmake option.

```
cmake . -DBUILD_POSIX_TOOLS -DBUILD_GATEWAY ...
```

You can also build the tools individually without whole project's cmake like this:

```
go build ./cmd/cvmfs_touch
```

Note: You must have the `cvmfs-swissknife` tool installed to use this code.

## Configuration Setup

The configuration for each repo should be located at `/etc/cvmfs/gateway-client/<repo-name>/cvmfs-rsync.yaml` and should match the structure of the file `doc/cvmfs_rsync_schema.yaml`.

## Usage

This toolkit strives to satisfy the expectations of the user familiar with respective common tools.
For more details, look at the readme at `cmd/<tool_name>` or run the tool with the `--help` flag.

## Testing

It is recommended to test each tool separately, to do so, read the testing section in the tool that you wish to test.

CVMFS build system builds separate executables named like `cvmfs_touch.test`, but does not install them.
Running these executables is equivalent to running `go test`.
