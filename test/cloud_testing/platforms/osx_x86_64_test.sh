#!/bin/sh

retval=0

#cvmfs_unittests --gtest_shuffle \
#                --gtest_death_test_use_fork || retval=1

logfile="$cvmfs_log_directory/integration_tests.log"

cd "$cvmfs_workspace"


# everything will be placed in the home folder
echo "running CernVM-FS client test cases..."
CVMFS_TEST_CLASS_NAME=ClientIntegrationTests
./run.sh "$logfile" -x        src/004-davinci                      \
                              src/005-asetup                       \
                              src/006-buildkernel                  \
                              src/007-testjobs                     \
                              src/008-default_domain               \
                              src/016-dnsunreachable               \
                              src/017-dnstimeout                   \
                              src/024-reload-during-asetup         \
                              src/039-reloadalarm                  \
                              src/040-aliencache                   \
                              src/045-oasis                        \
                              src/052-roundrobindns                \
                              src/055-ownership                    \
                              src/056-lowspeedlimit                \
                              src/057-parallelmakecache            \
                              src/061-systemdnokill                \
                              --                                   \
                              src/0*

retval=$?
exit $retval
