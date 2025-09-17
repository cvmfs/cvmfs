# cvmfs_ln
The cvmfs_ln tool.

This is a tool designed to be very much like ln, but designed to function with the cvmfs file system.

## Usage
```
$ cvmfs_ln --help

This is the cvmfs_ln tool, meant to implement ln functionality for the CVMFS file  system.

Usage: cvmfs_ln [OPTION]... TARGET LINK_NAME
cvmfs_ln [OPTION]... TARGET
cvmfs_ln [OPTION]... TARGET... DIRECTORY
TARGET and DEST must be in CVMFS.

Currently only designed for symlinks.

Memory Estimation:
Generally this will be <1GB.




Options
      --debug             Add debug logging.
  -n, --dry-run           Report on changes that would be made without uploading objects or making changes to CVMFS.
  -f, --force             Delete existing files.
  -N, --no-dereference    Do not dereference the final path component in ln processing (allows for symlinks pointing to dirs to be changed)
  -P, --priority string   Priority of cvmfs_rsync job (low, med, high) (Note: In rare cases, low and med jobs can be serviced before high priority jobs). THIS REQUIRES AN UPGRADED GATEWAY TO USE. Please reach out to linux to get this set up, or if you are unsure if your gateway is upgraded. (default "low")
  -s, --symbolic          Create a symbolic link instead of hard link.
```

## Testing

To test this code, you should run `TestTurboSpeed`, it encompasses the one test in this test suite:

`go test -run TestTurboSpeed`

Specifying `-run` and a regular expression will run every test with a name that matches that regular expression.

NOTE: This testing suite is designed to be grafting aware in order to be quicker to test. It is recommended to either run `go test -run TestTurboSpeed` to test everything. Simply running `go test` will run every test twice, and may not provide the same speed benefits due to lease contention.
