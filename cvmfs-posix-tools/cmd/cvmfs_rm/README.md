# cvmfs_rm
The cvmfs_rm tool.

This is a tool designed to be very much like rm, but designed to function with the cvmfs file system.

## Usage
```
$ cvmfs_rm --help

This is the cvmfs_rm tool, meant to implement rm functionality for the CVMFS file  system.

Usage: cvmfs_rm [OPTION]... FILE...
Tool must be called in a CVMFS directory.

Memory Estimation:
Generally this will be <1GB.




Options
      --core-allotment int   Automatically tunes your rsync run as if it had this number of cores alloted. Setting this value <1 instead (recommended) uses the number of cpus available as this value (maximum of 8, unless running in a slurm job which reads from slurm variable). Note, this is only applicable for purging. (default -1)
      --debug                Add debug logging.
  -n, --dry-run              Report on changes that would be made without uploading objects or making changes to CVMFS.
  -f, --file string          Remove the paths from a line separated list of paths relative to a provided root directory (a la cvmfs_insert). Ex. cvmfs_rm <repo_root> --file <path_file>
  -P, --priority string      Priority of cvmfs_rsync job (low, med, high) (Note: In rare cases, low and med jobs can be serviced before high priority jobs). THIS REQUIRES AN UPGRADED GATEWAY TO USE. Please reach out to linux to get this set up, or if you are unsure if your gateway is upgraded. (default "low")
      --purge                Purge directories.
  -r, --recursive            Remove dirs and contents recursively.
```

## Testing

To test this code, you should run `TestTurboSpeed`, it encompasses the one test in this test suite:

`go test -run TestTurboSpeed`

Specifying `-run` and a regular expression will run every test with a name that matches that regular expression.

NOTE: This testing suite is designed to be grafting aware in order to be quicker to test. It is recommended to either run `go test -run TestTurboSpeed` to test everything. Simply running `go test` will run every test twice, and may not provide the same speed benefits due to lease contention.
