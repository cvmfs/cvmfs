#!/bin/sh

export CVMFS_PLATFORM_NAME="centos9-x86_64_FUSE3"
export CVMFS_TIMESTAMP=$(date -u +'%Y-%m-%dT%H:%M:%SZ')

# source the common platform independent functionality and option parsing
script_location=$(cd "$(dirname "$0")"; pwd)
. ${script_location}/common_test.sh

retval=0

# Test exclusions
# 095: not needed on EL9 as attach mounts are supported

cd ${SOURCE_DIRECTORY}/test
echo "running CernVM-FS client test cases..."
CVMFS_TEST_CLASS_NAME=ClientIntegrationTests                                  \
./run.sh $CLIENT_TEST_LOGFILE -o ${CLIENT_TEST_LOGFILE}${XUNIT_OUTPUT_SUFFIX} \
                              -x src/005-asetup                               \
                                 src/004-davinci                              \
                                 src/007-testjobs                             \
                                 src/017-dnstimeout                           \
                                 src/019-httptimeout                          \
                                 src/020-emptyrepofailover                    \
                                 src/030-missingrootcatalog                   \
                                 src/056-lowspeedlimit                        \
                                 src/065-http-400                             \
                                 src/084-premounted                           \
                                 src/095-fuser                                \
                                 src/096-cancelreq                            \
                                 --                                           \
                                 src/0*                                       \
                                 src/1*                                       \
                              || retval=1


echo "running CernVM-FS client migration test cases..."
CVMFS_TEST_CLASS_NAME=ClientMigrationTests                        \
./run.sh $MIGRATIONTEST_CLIENT_LOGFILE                            \
         -o ${MIGRATIONTEST_CLIENT_LOGFILE}${XUNIT_OUTPUT_SUFFIX} \
            migration_tests/0*                                    \
         || retval=1

exit $retval
