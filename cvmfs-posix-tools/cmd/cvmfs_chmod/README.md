# cvmfs_chmod
The cvmfs_chmod tool.

This is a tool designed to be very much like chmod, but designed to function with the cvmfs file system.

A note about chmod - it is dot scheme aware. This means that if you are trying to change a dot scheme file, modifying the name of the file will modify the underlying file. The file cannot, however, be directly targeted.

Warning: This does not currently work with files for Content Addressable repos, use with caution.

## Usage
```
$ cvmfs_chmod --help

This is the cvmfs_chmod tool, meant to implement chmod functionality for the CVMFS file  system.

Usage: cvmfs_chmod [OPTION]... MODE[,MODE]... FILE...
cvmfs_chmod [OPTION]... OCTAL-MODE FILE...
cvmfs_chmod [OPTION]... --reference=RFILE FILE...
Tool must be called in a CVMFS directory.
A note about chmod - it is dot scheme aware. This means that if you are trying to change a dot scheme file, modifying the name of the file will modify the underlying file. The file cannot, however, be directly targeted.

Memory Estimation:
To estimate the total working memory necessary for your insert, the following formula will give you a good estimation in practice:
total_memory = 30MB + 8MB * core-allotment

Generally this will be <1GB.




Options
      --core-allotment int   Automatically tunes your rsync run as if it had this number of cores alloted. Setting this value <1 instead (recommended) uses the number of cpus available as this value (maximum of 8, unless running in a slurm job which reads from slurm variable) (default -1)
      --debug                Add debug logging.
  -f, --file string          chmod the paths from a line separated list of paths relative to a provided root directory (a la cvmfs_insert). Ex. cvmfs_chmod <mode> <repo_root> --file <path_file>
  -P, --priority string      Priority of cvmfs_rsync job (low, med, high) (Note: In rare cases, low and med jobs can be serviced before high priority jobs). THIS REQUIRES AN UPGRADED GATEWAY TO USE. Please reach out to linux to get this set up, or if you are unsure if your gateway is upgraded. (default "low")
  -R, --recursive            Change files and dirs recursively
      --reference string     Use RFILE's mode.
```

## Testing

To test this code, you should run `TestTurboSpeed`, it encompasses the one test in this test suite:

`go test -run TestTurboSpeed`

Specifying `-run` and a regular expression will run every test with a name that matches that regular expression.

NOTE: This testing suite is designed to be grafting aware in order to be quicker to test. It is recommended to either run `go test -run TestTurboSpeed` to test everything. Simply running `go test` will run every test twice, and may not provide the same speed benefits due to lease contention.
