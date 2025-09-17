# cvmfs_touch
The cvmfs_touch tool.

This is a tool designed to be very much like touch for existing files, but designed to function with the cvmfs file system.
This tool will not create empty files if the path does not exist, it will simply fail instead.

Warning: This does not currently work with files for Content Addressable repos, use with caution.

Warning: This tool is designed to be an INTERNAL tool, be cautious in sharing as it's main purpose is to cut through cache layers.

## Usage
```
$ cvmfs_touch --help

This is the cvmfs_touch tool, meant to implement touch functionality for the CVMFS file  system for existing files. This tool will not create an empty file, making it different than traditional touch.

Usage: cvmfs_touch [OPTION]... FILE...
Tool must be called in a CVMFS directory.

Memory Estimation:
To estimate the total working memory necessary for your insert, the following formula will give you a good estimation in practice:
total_memory = 30MB + 8MB * core-allotment

Generally this will be <1GB.




Options
      --core-allotment int   Automatically tunes your rsync run as if it had this number of cores alloted. Setting this value <1 instead (recommended) uses the number of cpus available as this value (maximum of 8, unless running in a slurm job which reads from slurm variable) (default -1)
      --debug                Add debug logging.
  -h, --no-dereference       Affect any symlinks instead of the files they reference.
  -P, --priority string      Priority of cvmfs_rsync job (low, med, high) (Note: In rare cases, low and med jobs can be serviced before high priority jobs). THIS REQUIRES AN UPGRADED GATEWAY TO USE. Please reach out to linux to get this set up, or if you are unsure if your gateway is upgraded. (default "low")
```

## Testing

To test this code, you should run `TestTurboSpeed`, it encompasses the one test in this test suite:

`go test -run TestTurboSpeed`

Specifying `-run` and a regular expression will run every test with a name that matches that regular expression.

NOTE: This testing suite is designed to be grafting aware in order to be quicker to test. It is recommended to either run `go test -run TestTurboSpeed` to test everything. Simply running `go test` will run every test twice, and may not provide the same speed benefits due to lease contention.
