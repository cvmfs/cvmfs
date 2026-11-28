# cvmfs_rmdir
The cvmfs_rmdir tool.

This is a tool designed to be very much like rmdir, but designed to function with the cvmfs file system.

## Usage
```
$ cvmfs_rmdir --help

This is the cvmfs_rmdir tool, meant to implement rmdir functionality for the CVMFS file  system.

Usage: cvmfs_rmdir [OPTION]... DIRECTORY...
Tool must be called in a CVMFS directory.

Memory Estimation:
Generally this will be <1GB.




Options
      --debug             Add debug logging.
  -n, --dry-run           Report on changes that would be made without uploading objects or making changes to CVMFS.
  -f, --file string       Remove the paths from a line separated list of paths relative to a provided root directory (a la cvmfs_insert). Ex. cvmfs_rmdir <repo_root> --file <path_file>
  -P, --priority string   Priority of cvmfs_rsync job (low, med, high) (Note: In rare cases, low and med jobs can be serviced before high priority jobs). THIS REQUIRES AN UPGRADED GATEWAY TO USE. Please reach out to linux to get this set up, or if you are unsure if your gateway is upgraded. (default "low")
```

## Testing

To test this code, you should run `TestTurboSpeed`, it encompasses the one test in this test suite:

`go test -run TestTurboSpeed`

Specifying `-run` and a regular expression will run every test with a name that matches that regular expression.

NOTE: This testing suite is designed to be grafting aware in order to be quicker to test. It is recommended to either run `go test -run TestTurboSpeed` to test everything. Simply running `go test` will run every test twice, and may not provide the same speed benefits due to lease contention.
