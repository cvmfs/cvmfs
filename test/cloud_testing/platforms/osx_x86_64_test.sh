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
CVMFS_TEST_CLASS_NAME=ClientIntegrationTests                                  \
sudo ./run.sh "$logfile"              src/000-dummy                                \
                                 src/001-chksetup                             \
                              || retval=1


exit $retval
