#!/bin/sh
set -x
retval=0

#cvmfs_unittests --gtest_shuffle \
#                --gtest_death_test_use_fork || retval=1

logfile="/var/log/cvmfs-test"
workdir="$HOME/cvmfs/test"

cd "$workdir"


# everything will be placed in the home folder
echo "running CernVM-FS client test cases..."
CVMFS_TEST_CLASS_NAME=ClientIntegrationTests
sudo ./run.sh "$logfile"  src/000-dummy                 \
                          src/001-chksetup              \
                          src/002-probe                 \
                          src/003-nested                \
                          src/009-tar                   \
                          src/010-du                    \
                          src/012-ls-s                  \
                          src/013-certificate_cache     \
                          src/014-corrupt_lru           \
                          src/015-rebuild_on_crash      \
                          src/017-dnstimeout            \
                          src/018-httpunreachable       \
                          src/019-httptimeout           \
                          src/020-emptyrepofailover     \
                          src/021-stacktrace


exit $retval
