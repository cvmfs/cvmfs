# cvmfs_chown
The cvmfs_chown tool.

This is a tool designed to be very much like chown, but designed to function with the cvmfs file system.

A note about chown - it is dot scheme aware. This means that if you are trying to change a dot scheme file, modifying the name of the file will modify the underlying file. The file cannot, however, be directly targeted.

Warning: This does not currently work with files for Content Addressable repos, use with caution.

## Usage
```
$ cvmfs_chown --help

This is the cvmfs_chown tool, meant to implement chown functionality for the CVMFS file  system.

Usage: cvmfs_chown [OPTION]... [OWNER[:GROUP]]... FILE...
cvmfs_chown [OPTION]... --reference=RFILE FILE...
Tool must be called in a CVMFS directory.
A note about chown - it is dot scheme aware. This means that if you are trying to change a dot scheme file, modifying the name of the file will modify the underlying file. The file cannot, however, be directly targeted.

Memory Estimation:
To estimate the total working memory necessary for your insert, the following formula will give you a good estimation in practice:
total_memory = 30MB + 8MB * core-allotment

Generally this will be <1GB.




Options
      --core-allotment int   Automatically tunes your rsync run as if it had this number of cores alloted. Setting this value <1 instead (recommended) uses the number of cpus available as this value (maximum of 8, unless running in a slurm job which reads from slurm variable) (default -1)
      --debug                Add debug logging.
  -f, --file string          chown the paths from a line separated list of directories relative to a provided root directory (a la cvmfs_insert). Ex. cvmfs_chown <owner:grp> <repo_root> --file <dir_file>
  -P, --priority string      Priority of cvmfs_rsync job (low, med, high) (Note: In rare cases, low and med jobs can be serviced before high priority jobs). THIS REQUIRES AN UPGRADED GATEWAY TO USE. Please reach out to linux to get this set up, or if you are unsure if your gateway is upgraded. (default "low")
  -R, --recursive            Change files and dirs recursively
      --reference string     Use RFILE's owner.
```

## Testing

There is only one test for this tool (it encompasses all expected behavior). To run it, run:

`go test`
