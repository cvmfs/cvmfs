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
sudo ./run.sh "$logfile"  000-dummy                 \
                          001-chksetup              \
                          002-probe                 \
                          003-nested                \
                          004-davinci               \
                          005-asetup                \
                          007-testjobs              \
                          008-default_domain        \
                          009-tar                   \
                          010-du                    \
                          012-ls-s                  \
                          013-certificate_cache     \
                          014-corrupt_lru           \
                          017-dnstimeout            \
                          018-httpunreachable       \
                          019-httptimeout


exit $retval
