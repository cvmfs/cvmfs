# A few notes on how to run cvmfs under valgrind

  - build with valgrind-devel
  - no autofs
  - use `-o simple_options_parsing`
    (no qotes for CVMFS_HTTP_PROXY and CVMFS_SERVER_URL in the options)
  - use `-o disable_watchdog`
  - user root (`-o gid=0,uid=0`)
  - no shared cache to avoid forking the quota manager, CVMFS_QUOTA_LIMIT=-1 to disable quota sqlite handling
  - foreground mount (`cvmfs2 -f`)

## Example

    ulimit -n 66000
    valgrind /usr/bin/cvmfs2 --tool=massif --alloc-fn=smalloc --stacks=yes --threshold=0.01 \
      /usr/bin/cvmfs2 -f -o \
      disable_watchdog,simple_options_parsing,fsname=cvmfs2,allow_other,grab_mountpoint,uid=0,gid=0 \
      sft.cern.ch /cvmfs/sft.cern.ch

(or with option --pages-as-heap=yes instead of --stacks=yes)

