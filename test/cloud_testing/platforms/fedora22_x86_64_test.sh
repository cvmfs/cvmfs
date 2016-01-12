#!/bin/sh

# source the common platform independent functionality and option parsing
script_location=$(cd "$(dirname "$0")"; pwd)
. ${script_location}/common_test.sh

retval=0

# running unit test suite
run_unittests --gtest_shuffle \
              --gtest_death_test_use_fork || retval=1


cd ${SOURCE_DIRECTORY}/test
echo "running CernVM-FS client test cases..."
CVMFS_TEST_CLASS_NAME=ClientIntegrationTests                                  \
./run.sh $CLIENT_TEST_LOGFILE -o ${CLIENT_TEST_LOGFILE}${XUNIT_OUTPUT_SUFFIX} \
                                 --                                           \
                                 src/0*                                       \
                              || retval=1


echo "running CernVM-FS server test cases..."
CVMFS_TEST_SERVER_CACHE='/srv/cache'                                          \
CVMFS_TEST_CLASS_NAME=ServerIntegrationTests                                  \
./run.sh $SERVER_TEST_LOGFILE -o ${SERVER_TEST_LOGFILE}${XUNIT_OUTPUT_SUFFIX} \
                              -x src/518-hardlinkstresstest                   \
                                 src/523-corruptchunkfailover                 \
                                 src/524-corruptmanifestfailover              \
                                 src/577-garbagecollecthiddenstratum1revision \
                                 src/579-garbagecollectstratum1legacytag      \
                                 src/585-xattrs                               \
                                 --                                           \
                                 src/5*                                       \
                              || retval=1


echo "running CernVM-FS migration test cases..."
CVMFS_TEST_CLASS_NAME=MigrationTests                                              \
./run.sh $MIGRATIONTEST_LOGFILE -o ${MIGRATIONTEST_LOGFILE}${XUNIT_OUTPUT_SUFFIX} \
                                   migration_tests/*                              \
                                || retval=1

exit $retval
