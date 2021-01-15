#!/bin/sh

export CVMFS_PLATFORM_NAME="centos8-x86_64"
export CVMFS_TIMESTAMP=$(date -u +'%Y-%m-%dT%H:%M:%SZ')

# source the common platform independent functionality and option parsing
script_location=$(cd "$(dirname "$0")"; pwd)
. ${script_location}/common_test.sh

retval=0

# allow apache access to the mounted server file system
echo -n "setting SELinux labels for apache... "
sudo chcon -Rv --type=httpd_sys_content_t /srv > /dev/null || die "fail"
echo "done"

# start apache
echo -n "starting apache... "
sudo systemctl start httpd > /dev/null 2>&1 || die "fail"
echo "OK"

# running unit test suite
run_unittests --gtest_shuffle \
              --gtest_death_test_use_fork || retval=1

cd ${SOURCE_DIRECTORY}/test
echo "running CernVM-FS client test cases..."
CVMFS_TEST_CLASS_NAME=ClientIntegrationTests                                  \
./run.sh $CLIENT_TEST_LOGFILE -o ${CLIENT_TEST_LOGFILE}${XUNIT_OUTPUT_SUFFIX} \
                              -x src/005-asetup                               \
                                 src/004-davinci                              \
                                 src/007-testjobs                             \
                                 src/056-lowspeedlimit                        \
                                 src/084-premounted                           \
                                 --                                           \
                                 src/0*                                       \
                              || retval=1


echo "running CernVM-FS server test cases..."
CVMFS_TEST_CLASS_NAME=ServerIntegrationTests                                  \
CVMFS_TEST_UNIONFS=overlayfs                                                  \
./run.sh $SERVER_TEST_LOGFILE -o ${SERVER_TEST_LOGFILE}${XUNIT_OUTPUT_SUFFIX} \
                              -x src/518-hardlinkstresstest                   \
                                 src/600-securecvmfs                          \
                                 src/628-pythonwrappedcvmfsserver             \
                                 src/672-publish_stats_hardlinks              \
                                 src/673-acl                                  \
                                 --                                           \
                                 src/5*                                       \
                                 src/6*                                       \
                                 src/7*                                       \
                                 src/8*                                       \
                                 src/9*                                       \
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

echo "running DUCC test cases..."
CVMFS_TEST_CLASS_NAME=DUCCTests                                         \
./run.sh $DUCCTEST_LOGFILE -o ${DUCCTEST_LOGFILE}${XUNIT_OUTPUT_SUFFIX} \
                                   src/4*                               \
                                || retval=1

exit $retval
