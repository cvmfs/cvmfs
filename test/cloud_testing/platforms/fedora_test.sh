#!/bin/sh

export CVMFS_PLATFORM_NAME="fedora$(. /etc/os-release && echo "$VERSION_ID")-$(uname -m)" 
export CVMFS_TIMESTAMP=$(date -u +'%Y-%m-%dT%H:%M:%SZ')

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
                                 src/539-symlinkedvarspoolcvmfs               \
                                 src/600-securecvmfs                          \
                                 src/628-pythonwrappedcvmfsserver             \
                                 src/647-bearercvmfs                          \
                                 src/672-publish_stats_hardlinks              \
                                 --                                           \
                                 src/5*                                       \
                                 src/6*                                       \
                                 src/7*                                       \
                              || retval=1


echo "running CernVM-FS client migration test cases..."
CVMFS_TEST_CLASS_NAME=ClientMigrationTests                        \
./run.sh $MIGRATIONTEST_CLIENT_LOGFILE                            \
         -o ${MIGRATIONTEST_CLIENT_LOGFILE}${XUNIT_OUTPUT_SUFFIX} \
            migration_tests/0*                                    \
         || retval=1


echo "running CernVM-FS server migration test cases..."
CVMFS_TEST_CLASS_NAME=ServerMigrationTests                        \
./run.sh $MIGRATIONTEST_SERVER_LOGFILE                            \
         -o ${MIGRATIONTEST_SERVER_LOGFILE}${XUNIT_OUTPUT_SUFFIX} \
            migration_tests/5*                                    \
         || retval=1

exit $retval
