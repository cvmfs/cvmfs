# cvmfs_mkdir
The cvmfs_mkdir tool.

This is a tool designed to be very much like mkdir, but designed to function with the cvmfs file system.

## Usage
```
$ cvmfs_mkdir --help

This is the cvmfs_mkdir tool, meant to implement mkdir functionality for the CVMFS file  system.

Usage: cvmfs_mkdir [OPTION]... DIRECTORY...
Tool must be called in a CVMFS directory.

Memory Estimation:
Generally this will be <1GB.




Options
      --debug             Add debug logging.
  -n, --dry-run           Report on changes that would be made without uploading objects or making changes to CVMFS.
  -f, --file string       Create the directories from a line separated list of directories relative to a provided root directory (a la cvmfs_insert). Ex. cvmfs_mkdir <repo_root> --file <dir_file>
  -m, --mode string       Create directory with given mode arguments.
  -p, --parent            Create parent directories.
  -P, --priority string   Priority of cvmfs_rsync job (low, med, high) (Note: In rare cases, low and med jobs can be serviced before high priority jobs). THIS REQUIRES AN UPGRADED GATEWAY TO USE. Please reach out to linux to get this set up, or if you are unsure if your gateway is upgraded. (default "low")
```

## Testing

To test this code, you should run `TestTurboSpeed`, it encompasses the one test in this test suite:

`go test -run TestTurboSpeed`

Specifying `-run` and a regular expression will run every test with a name that matches that regular expression.

NOTE: This testing suite is designed to be grafting aware in order to be quicker to test. It is recommended to either run `go test -run TestTurboSpeed` to test everything. Simply running `go test` will run every test twice, and may not provide the same speed benefits due to lease contention.
