# A few notes on how to run cvmfs under valgrind

  - build with valgrind-devel
  - no autofs
  - use `-o simple_options_parsing`
    (no qotes for CVMFS_HTTP_PROXY and CVMFS_SERVER_URL in the options)
  - use `-o disable_watchdog`
  - user root (`-o gid=0,uid=0`)
  - no shared cache to avoid forking the quota manager
  - foreground mount (`cvmfs2 -f`)

## Example

    valgrind /usr/bin/cvmfs2 --tool=massif --stacks=yes --peak-inaccuracy=0.1 -f -o \
      disable_watchdog,simple_options_parsing,fsname=cvmfs2,allow_other,grab_mountpoint,uid=0,gid=0 \
      sft.cern.ch /cvmfs/sft.cern.ch

