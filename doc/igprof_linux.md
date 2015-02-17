##Notes for IgProf profiler tool in Linux

Due to various clashes between IgProf and some old Linux versions it is necessary to do some previous configuration in order to run IgProf. It looks like the problem is caused when there is a fork during the execution.
These are the steps to solve it:

1. Remove the /etc/mtab file
2. Create a symbolic link in /etc/mtab pointing to /proc/mounts
3. Execute cvmfs2 with the following options (using the -o flag):
    * disable_watchdog
    * fast_parse
    * uid=0
    * gid=0
4. Also, include the -f flag to avoid a fork.


The following code runned as superuser would sum up what was explained before:

``` bash
mv /etc/mtab /etc/mtab.disabled
ln -s /proc/mounts /etc/mtab
igprof -d -t cvmfs2 -pp -z -o < your_compress_igprof_log_file > cvmfs2 -f -o config=< your_config_file >,disable_watchdog,fast_parse,uid=0,gid=0 < your_server > < your_mount_point >

```
