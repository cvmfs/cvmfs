#!/bin/sh

export CVMFS_PLATFORM_NAME="macos"
export CVMFS_TIMESTAMP=$(date -u +'%Y-%m-%dT%H:%M:%SZ')

export PATH=/usr/local/bin:$PATH

# source the common platform independent functionality and option parsing
script_location=$(cd "$(dirname "$0")"; pwd)
. ${script_location}/common_test.sh

retval=0

#cvmfs_unittests --gtest_shuffle \
#                --gtest_death_test_use_fork || retval=1

cd ${SOURCE_DIRECTORY}/test

# everything will be placed in the home folder
echo "running CernVM-FS client test cases..."
CVMFS_TEST_CLASS_NAME=ClientIntegrationTests
./run.sh "$CLIENT_TEST_LOGFILE" -o ${CLIENT_TEST_LOGFILE}${XUNIT_OUTPUT_SUFFIX} \
                                -x src/004-davinci                              \
                                   src/005-asetup                               \
                                   src/006-buildkernel                          \
                                   src/007-testjobs                             \
                                   src/008-default_domain                       \
                                   src/016-dnsunreachable                       \
                                   src/017-dnstimeout                           \
                                   src/024-reload-during-asetup                 \
                                   src/039-reloadalarm                          \
                                   src/040-aliencache                           \
                                   src/045-oasis                                \
                                   src/052-roundrobindns                        \
                                   src/050-configrepo                           \
                                   src/055-ownership                            \
                                   src/056-lowspeedlimit                        \
                                   src/057-parallelmakecache                    \
                                   src/061-systemdnokill                        \
                                   src/074-oom                                  \
                                   src/081-shrinkwrap                           \
                                   src/082-shrinkwrap-cms                       \
                                   src/083-suid                                 \
                                   src/084-premounted                           \
                                   src/089-external_cache_plugin                \
                                   --                                           \
                                   src/0*

retval=$?
exit $retval
