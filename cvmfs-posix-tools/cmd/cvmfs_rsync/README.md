# cvmfs_rsync
The cvmfs_rsync tool.

This is a tool designed to be very much like rsync, but designed to function with the cvmfs file system.

## Usage
```
$ cvmfs_rsync --help

This is the cvmfs_rsync tool, meant to implement rsync functionality for the CVMFS file system.

Usage: cvmfs_rsync [OPTION] ... SRC DEST
Either SRC or DEST must be in CVMFS, but not both.

A special note about rsync. When copying directories, path/to/dir will copy the directory as is into DEST,
whereas /path/to/dir/ will only copy the directory contents into DEST

Memory Estimation:
To estimate the total working memory necessary for your rsync, the following formula will give you a good estimation in practice:
total_memory = 48MB * core-allotment




Options
  -a, --acls ACLFlag              ACL preservation: preserve-all (previous behaviour), preserve-mode, preserve-execute, preserve-owner, none (default) (default none)
      --changelog string          Takes a <file> argument. Create a log file containing structured data for all changes made in <insert_format>.
  -c, --checksum                  Compare using a full file checksum rather than modtime + size.
  -L, --copy-links                Dereference symlinks when copying from non-CVMFS to CVMFS.
      --core-allotment int        Automatically tunes your rsync run as if it had this number of cores alloted. Setting this value <1 instead (recommended) uses the number of cpus available as this value (maximum of 8, unless running in a slurm job which reads from slurm variable) (default -1)
      --debug                     Sets log level to debug.
      --delete                    Removes files from DEST that are not present in SRC. By default no files will be removed.
  -d, --dirs                      Perform cvmfs_rsync transferring dirs without recursing.
  -n, --dry-run                   Report on changes that would be made without uploading objects or making changes to CVMFS.
      --exclude strings           Exclude files matching one or more shell file name patterns i.e. --exclude=<pattern> --exclude=<pattern>. Patterns are applied in order from left to right. Quote <pattern> to avoid shell expansion prior to argument processing.
  -f, --file string               Perform the rsync from a line separated list of directories relative to a provided src directory (a la --files-from in traditional rsync). Ex. cvmfs_rsync --file=<file_of_paths> <src_root> <dest>. WARNING: Does not work for absolute paths, --relative flag will be based on file contents, not src root.
  -P, --priority string           Priority of cvmfs_rsync job (low, med, high) (Note: In rare cases, low and med jobs can be serviced before high priority jobs). THIS REQUIRES AN UPGRADED GATEWAY TO USE. Please reach out to linux to get this set up, or if you are unsure if your gateway is upgraded. (default "low")
      --purge                     Remove corresponding objects when files are deleted. Requires --delete.
  -r, --recursive                 Perform cvmfs_rsync recursively into SRC.
  -R, --relative                  Perform cvmfs_rsync with relative path names.
      --retry-changed-files       Allows files to be re-uploaded if they change during the rsync run (default true)
      --run-profiling-webserver   Run a webserver that runs a profiler.
```

## Testing

To test this code, you can either test the whole test suite, or a subset of it. Here is the command to do that:

`go test [-run regexp]`

Specifying `-run` and a regular expression will run every test with a name that matches that regular expression.

NOTE: This testing suite is designed to be grafting aware in order to be quicker to test. It is recommended to either run `go test -run TestTurboSpeed` to test everything (TestTurboSpeed encompasses every test), or specify which tests you want to run. Simply running `go test` will run every test twice, and may not provide the same speed benefits due to lease contention.
