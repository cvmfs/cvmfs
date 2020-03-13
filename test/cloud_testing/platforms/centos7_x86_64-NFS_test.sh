#!/bin/sh

# source the common platform independent functionality and option parsing
script_location=$(cd "$(dirname "$0")"; pwd)
. ${script_location}/common_test.sh

retval=0

cd ${SOURCE_DIRECTORY}/test
echo "running CernVM-FS client test cases..."
CVMFS_TEST_CLASS_NAME=ClientIntegrationTests                                  \
./run.sh $CLIENT_TEST_LOGFILE -o ${CLIENT_TEST_LOGFILE}${XUNIT_OUTPUT_SUFFIX} \
                              -x src/001-chksetup                             \
                                 src/005-asetup                               \
                                 src/004-davinci                              \
                                 src/007-testjobs                             \
                                 src/026-tightcache                           \
                                 src/032-workspace                            \
                                 src/040-aliencache                           \
                                 src/041-rocache                              \
                                 src/043-highinodes                           \
                                 src/068-rocache                              \
                                 src/070-tieredcache                          \
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
