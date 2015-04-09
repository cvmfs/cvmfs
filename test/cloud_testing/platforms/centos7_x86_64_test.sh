#!/bin/sh

# source the common platform independent functionality and option parsing
script_location=$(dirname $(readlink --canonicalize $0))
. ${script_location}/common_test.sh

ut_retval=0
it_retval=0
s3_retval=0
mg_retval=0

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
              --gtest_death_test_use_fork || ut_retval=$?

echo "running CernVM-FS test cases..."
cd ${SOURCE_DIRECTORY}/test
export CVMFS_TEST_SERVER_CACHE='/srv/cache' &&         \
./run.sh $TEST_LOGFILE -x src/005-asetup               \
                          src/004-davinci              \
                          src/007-testjobs             \
                          src/024-reload-during-asetup \
                          src/5* || it_retval=$?

# TODO: enable this as soon as 2.1.20 is released
echo "skipping CernVM-FS migration test case (no EL7 package for 2.1.19)"
#./run.sh $MIGRATIONTEST_LOGFILE migration_tests/001-hotpatch || mg_retval=$?

[ $ut_retval -eq 0 ] && [ $it_retval -eq 0 ] && [ $s3_retval -eq 0 ] && [ $mg_retval -eq 0 ]
