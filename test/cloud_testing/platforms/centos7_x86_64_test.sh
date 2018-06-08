#!/bin/sh

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
# run_unittests --gtest_shuffle \
#               --gtest_death_test_use_fork || retval=1

cd ${SOURCE_DIRECTORY}/test
# echo "running CernVM-FS client test cases..."
# CVMFS_TEST_CLASS_NAME=ClientIntegrationTests                                  \
# ./run.sh $CLIENT_TEST_LOGFILE -o ${CLIENT_TEST_LOGFILE}${XUNIT_OUTPUT_SUFFIX} \
#                               -x src/005-asetup                               \
#                                  src/004-davinci                              \
#                                  src/007-testjobs                             \
#                                  --                                           \
#                                  src/0*                                       \
#                               || retval=1


# echo "running CernVM-FS server test cases..."
# CVMFS_TEST_CLASS_NAME=ServerIntegrationTests                                  \
# CVMFS_TEST_UNIONFS=overlayfs                                                  \
# ./run.sh $SERVER_TEST_LOGFILE -o ${SERVER_TEST_LOGFILE}${XUNIT_OUTPUT_SUFFIX} \
#                               -x src/518-hardlinkstresstest                   \
#                                  src/585-xattrs                               \
#                                  src/600-securecvmfs                          \
#                                  src/602-libcvmfs                             \
#                                  src/628-pythonwrappedcvmfsserver             \
#                                  --                                           \
#                                  src/5*                                       \
#                                  src/6*                                       \
#                                  src/7*                                       \
#                                  src/8*                                       \
#                               || retval=1

echo -n "starting the test S3 provider... "
s3_retval=0
test_s3_pid=$(start_test_s3 $TEST_S3_LOGFILE) || { s3_retval=1; retval=1; echo "fail"; }
echo "done ($test_s3_pid)"
create_test_s3_bucket

if [ $s3_retval -eq 0 ]; then
  echo "running CernVM-FS server test cases against the test S3 provider..."
  CVMFS_TEST_S3_CONFIG=$TEST_S3_CONFIG                                      \
  CVMFS_TEST_HTTP_BASE=$TEST_S3_URL                                         \
  CVMFS_TEST_CLASS_NAME=S3ServerIntegrationTests                            \
  ./run.sh $S3_TEST_LOGFILE -o ${S3_TEST_LOGFILE}${XUNIT_OUTPUT_SUFFIX}     \
                            -x src/518-hardlinkstresstest                   \
                               src/519-importlegacyrepo                     \
                               src/522-missingchunkfailover                 \
                               src/523-corruptchunkfailover                 \
                               src/525-bigrepo                              \
                               src/528-recreatespoolarea                    \
                               src/530-recreatespoolarea_defaultkey         \
                               src/537-symlinkedbackend                     \
                               src/538-symlinkedstratum1backend             \
                               src/542-storagescrubbing                     \
                               src/543-storagescrubbing_scriptable          \
                               src/550-livemigration                        \
                               src/563-garbagecollectlegacy                 \
                               src/568-migratecorruptrepo                   \
                               src/571-localbackendumask                    \
                               src/572-proxyfailover                        \
                               src/583-httpredirects                        \
                               src/585-xattrs                               \
                               src/591-importrepo                           \
                               src/594-backendoverwrite                     \
                               src/595-geoipdbupdate                        \
                               src/600-securecvmfs                          \
                               src/605-resurrectancientcatalog              \
                               src/607-noapache                             \
                               src/608-infofile                             \
                               src/610-altpath                              \
                               src/614-geoservice                           \
                               src/622-gracefulrmfs                         \
                               src/626-cacheexpiry                          \
                               src/700-overlayfsvalidation                  \
                               --                                           \
                               src/500*                                     \
                               || retval=1

  echo -n "Stopping the test S3 provider... "
  sudo kill -2 $test_s3_pid && echo "done" || echo "fail"
fi


# echo "running CernVM-FS migration test cases..."
# CVMFS_TEST_CLASS_NAME=MigrationTests                                              \
# ./run.sh $MIGRATIONTEST_LOGFILE -o ${MIGRATIONTEST_LOGFILE}${XUNIT_OUTPUT_SUFFIX} \
#                                    migration_tests/*                              \
#                                 || retval=1

exit $retval
