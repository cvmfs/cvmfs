#!/bin/sh

export CVMFS_PLATFORM_NAME="centos7-x86_64_EXCLCACHE"
export CVMFS_TIMESTAMP=$(date -u +'%Y-%m-%dT%H:%M:%SZ')

# source the common platform independent functionality and option parsing
script_location=$(cd "$(dirname "$0")"; pwd)
. ${script_location}/common_test.sh

retval=0

# run tests
cd ${SOURCE_DIRECTORY}/test
echo "running CernVM-FS client test cases..."
CVMFS_TEST_CLASS_NAME=ClientIntegrationTests                                  \
./run.sh $CLIENT_TEST_LOGFILE -o ${CLIENT_TEST_LOGFILE}${XUNIT_OUTPUT_SUFFIX} \
                              -x src/005-asetup                               \
                                 src/004-davinci                              \
                                 src/007-testjobs                             \
                                 src/035-unpinumount                          \
                                 src/042-cleanuppipes                         \
                                 src/081-shrinkwrap                           \
                                 src/082-shrinkwrap-cms                       \
                                 src/084-premounted                           \
                                 src/089-external_cache_plugin                \
                                 --                                           \
                                 src/0*                                       \
                              || retval=1

echo "running CernVM-FS client migration test cases..."
CVMFS_TEST_CLASS_NAME=ClientMigrationTests                        \
./run.sh $MIGRATIONTEST_CLIENT_LOGFILE                            \
         -o ${MIGRATIONTEST_CLIENT_LOGFILE}${XUNIT_OUTPUT_SUFFIX} \
            migration_tests/0*                                    \
          || retval=1

exit $retval
