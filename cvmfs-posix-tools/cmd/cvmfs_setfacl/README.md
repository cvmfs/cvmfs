# cvmfs_setfacl
The cvmfs_setfacl tool.

This is a tool designed to be very much like setfacl, but designed to function with the cvmfs file system.

## Usage
```
$ cvmfs_setfacl --help

This is the cvmfs_setfacl tool, meant to implement setfacl functionality for the CVMFS file  system.

Usage: cvmfs_setfacl FACL-FILE DIRECTORY...
Tool must be called in a CVMFS directory.

Note: cvmfs does not store acls on files themselves. This tool will only modify the acls of directories, and skip other file types.

Memory Estimation:
Generally this will be <1GB.




Options
      --debug             Add debug logging.
  -f, --file string       set the facl of the paths from a line separated list of paths relative to a provided root directory (a la cvmfs_insert). Ex. cvmfs_setfacl <facl_file> <repo_root> --file <path_file>
  -m, --modify string     modify the current ACL(s) of dir(s). Note, this replaces the need to specify a facl-file. E.x. cvmfs_setfacl -m <mod_string> <dirs>... Note: must be of the form <u,g>:<user or group>:<r/w/x>
  -P, --priority string   Priority of cvmfs_rsync job (low, med, high) (Note: In rare cases, low and med jobs can be serviced before high priority jobs). THIS REQUIRES AN UPGRADED GATEWAY TO USE. Please reach out to linux to get this set up, or if you are unsure if your gateway is upgraded. (default "low")
  -R, --recursive         Change files and dirs recursively
  -x, --remove string     remove entries from the current ACL(s) of dir(s). Note, this replaces the need to specify a facl-file. E.x. cvmfs_setfacl -x <mod_string> <dirs>... Note: must be of the form <u,g>:<user or group>:---
  -b, --remove-all        remove all extended entries from the current ACL(s) of dir(s). Note, this replaces the need to specify a facl-file. E.x. cvmfs_setfacl -b <dirs>...
```

## Testing

To test this code, you should run `TestTurboSpeed`, it encompasses the one test in this test suite:

`go test -run TestTurboSpeed`

Specifying `-run` and a regular expression will run every test with a name that matches that regular expression.

NOTE: This testing suite is designed to be grafting aware in order to be quicker to test. It is recommended to either run `go test -run TestTurboSpeed` to test everything. Simply running `go test` will run every test twice, and may not provide the same speed benefits due to lease contention.
