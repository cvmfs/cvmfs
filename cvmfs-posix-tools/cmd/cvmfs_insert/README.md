# cvmfs_insert
The cvmfs_insert tool.

This is a tool designed to upload files directly as specified by a csv, allowing for easy specification of many different srcs and dests.

## Usage
```
$ cvmfs_insert --help

This is the cvmfs_insert tool, meant to implement bulk inserts for the CVMFS file system.

Usage: cvmfs_insert [OPTION]... CVMFS_REPO FILE

FILE is a CSV file containing the list of files to upload and the relative destinations in the CVMFS repository.


Example:
insert,<source path 1>,<relative dest path 1>
insert,<source path 2>,<relative dest path 2>
...
insert,<source path X>,<relative dest path Y>


If the source path is a directory, a new directory will created if non-existant at destination

FILE can also contain facl paths which will take in a path to a file describing an acl (similar to cvmfs_setfacl) and a directory and
apply that facl to that directory.


Example:
facl,<facl path 1>,<dest path 1>
facl,<facl path 2>,<dest path 2>
...
facl,<facl path X>,<dest path Y>

NOTE: facl ONLY works with dirs. The --no-dereference flag does NOT work for this feature.
Duplicate dest entries will take the FIRST acl file supplied for that destination.


Memory Estimation:
To estimate the total working memory necessary for your insert, the following formula will give you a good estimation in practice:
total_memory = 48MB * core-allotment


Options
  -a, --acls ACLFlag          ACL preservation: preserve-all (previous behaviour), preserve-mode, preserve-execute, preserve-owner, none (default) (default none)
      --core-allotment int    Automatically tunes your insert run as if it had this number of cores alloted. Setting this value <1 instead (recommended) uses the number of cpus available as this value (maximum of 8, unless running in a slurm job which reads from slurm variable) (default -1)
      --debug                 Add debug logging.
  -n, --dry-run               Report on changes that would be made without uploading objects or making changes to CVMFS.
  -N, --no-dereference        Do not dereference the final path component in ln processing (allows for symlinks pointing to dirs to be changed)
  -P, --priority string       Priority of cvmfs_rsync job (low, med, high) (Note: In rare cases, low and med jobs can be serviced before high priority jobs). THIS REQUIRES AN UPGRADED GATEWAY TO USE. Please reach out to linux to get this set up, or if you are unsure if your gateway is upgraded. (default "low")
      --retry-changed-files   Allows files to be re-uploaded if they change during the insert run (default true)
      --skip-graft            Skip grafting step and preserve graft db.
```

## Testing

To test this code, you can either test the whole test suite, or a subset of it. Here is the command to do that:

`go test [-run regexp]`

Specifying `-run` and a regular expression will run every test with a name that matches that regular expression.

NOTE: This testing suite is designed to be grafting aware in order to be quicker to test. It is recommended to either run `go test -run TestTurboSpeed` to test everything (TestTurboSpeed encompasses every test), or specify which tests you want to run. Simply running `go test` will run every test twice, and may not provide the same speed benefits due to lease contention.
