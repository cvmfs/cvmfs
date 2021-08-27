#!/bin/sh

export CVMFS_PLATFORM_NAME="slc6-x86_64_S3"
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
sudo service httpd start > /dev/null 2>&1 || die "fail"
echo "OK"

# reset SELinux context
echo -n "restoring SELinux context for /var/lib/cvmfs... "
sudo restorecon -R /var/lib/cvmfs || die "fail"
echo "done"

# running unit test suite
run_unittests --gtest_shuffle \
              --gtest_death_test_use_fork || retval=1


cd ${SOURCE_DIRECTORY}/test

echo -n "starting the test S3 provider... "
s3_retval=0
test_s3_pid=$(start_test_s3 $TEST_S3_LOGFILE) || { s3_retval=1; retval=1; echo "fail"; }
echo "done ($test_s3_pid)"
create_test_s3_bucket

if [ $s3_retval -eq 0 ]; then
  echo "running CernVM-FS server test cases against the test S3 provider..."
  CVMFS_TEST_S3_CONFIG=$TEST_S3_CONFIG                                      \
  CVMFS_TEST_HTTP_BASE=$TEST_S3_URL                                         \
  CVMFS_TEST_S3_STORAGE=$TEST_S3_STORAGE/data/$TEST_S3_BUCKET               \
  CVMFS_TEST_CLASS_NAME=S3ServerIntegrationTests                            \
  ./run.sh $S3_TEST_LOGFILE -o ${S3_TEST_LOGFILE}${XUNIT_OUTPUT_SUFFIX}     \
                            -x src/518-hardlinkstresstest                   \
                               src/519-importlegacyrepo                     \
                               src/522-missingchunkfailover                 \
                               src/523-corruptchunkfailover                 \
                               src/524-corruptmanifestfailover              \
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
                               src/584-interleavingsnapshot                 \
                               src/585-xattrs                               \
                               src/591-importrepo                           \
                               src/594-backendoverwrite                     \
                               src/595-geoipdbupdate                        \
                               src/599-removehardlinks                      \
                               src/600-securecvmfs                          \
                               src/605-resurrectancientcatalog              \
                               src/607-noapache                             \
                               src/608-infofile                             \
                               src/609-metainfofile                         \
                               src/610-altpath                              \
                               src/614-geoservice                           \
                               src/622-gracefulrmfs                         \
                               src/647-bearercvmfs                          \
                               src/670-listreflog                           \
                               src/673-acl                                  \
                               src/682-enter                                \
                               --                                           \
                               src/5*                                       \
                               src/6*                                       \
                               src/8*                                       \
                               || retval=1

  echo -n "Stopping the test S3 provider... "
  sudo kill -2 $test_s3_pid && echo "done" || echo "fail"
fi

exit $retval
