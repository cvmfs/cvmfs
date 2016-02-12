##Notes for IgProf profiler tool in Linux

We have found out some issues with IgProf when CernVM-FS forks.
These are the steps to solve it:

1. Remove the /etc/mtab file
2. Create a symbolic link in /etc/mtab pointing to /proc/mounts
3. Execute cvmfs2 with the following options (using the -o flag):
    * disable_watchdog
    * simple\_options\_parsing
4. Also, include the -f flag to avoid a fork.

Some more notes:

  - Install igprof and libunwind from latest git code
  - Make sure libunwind and libigprof are also in /usr/lib64
  - Memory profiling needs to use the debug library for proper stack traces

The following code runned as superuser would sum up what was explained before:

``` bash
mv /etc/mtab /etc/mtab.disabled
ln -s /proc/mounts /etc/mtab
igprof -d -t cvmfs2 -pp -z -o < your_compress_igprof_log_file > cvmfs2 -f -o debug,config=< your_config_file >,disable_watchdog,simple_options_parsing < your_server > < your_mount_point >

```
