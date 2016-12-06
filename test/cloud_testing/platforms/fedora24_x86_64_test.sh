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
                              -x src/004-davinci                              \
                                 src/005-asetup                               \
                                 src/006-buildkernel                          \
                                 src/007-testjobs                             \
                                 src/024-reload-during-asetup                 \
                                 --                                           \
                                 src/0*                                       \
                              || retval=1

echo -n "make sure apache is running... "
sudo systemctl start httpd > /dev/null && echo "done" || echo "fail"

echo "running CernVM-FS server test cases..."
CVMFS_TEST_CLASS_NAME=ServerIntegrationTests                                  \
./run.sh $SERVER_TEST_LOGFILE -o ${SERVER_TEST_LOGFILE}${XUNIT_OUTPUT_SUFFIX} \
                              -x src/518-hardlinkstresstest                   \
                                 src/524-corruptmanifestfailover              \
                                 src/600-securecvmfs                          \
                                 --                                           \
                                 src/5*                                       \
                                 src/6*                                       \
                                 src/7*                                       \
                              || retval=1


# To do: remove me once previous package is available
echo "NOT running CernVM-FS migration test cases (disabled)..."
#CVMFS_TEST_CLASS_NAME=MigrationTests                                              \
#./run.sh $MIGRATIONTEST_LOGFILE -o ${MIGRATIONTEST_LOGFILE}${XUNIT_OUTPUT_SUFFIX} \
#                                   migration_tests/*                              \
#                                || retval=1

exit $retval
