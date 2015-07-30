#!/bin/sh

# source the common platform independent functionality and option parsing
script_location=$(cd "$(dirname "$0")"; pwd)
. ${script_location}/common_test.sh

# look for the ephemeral storage mount point
dev="\/dev\/vdb" # mind the escape!
ephemeral=$(sed -e "s/^$dev \(\/[^ ]*\) .*$/\1/;tx;d;:x" /proc/mounts)
[ ! -z $ephemeral ] || die "no ephemeral storage found on $dev"

# configure the ephemeral storage
custom_cache_dir="${ephemeral}/cvmfs_server_cache"
sudo chmod a+w "$ephemeral" || die "couldn't chmod storage at $ephemeral"
mkdir "$custom_cache_dir"   || die "couldn't create cache dir $custom_cache_dir"

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
                                 src/024-reload-during-asetup                 \
                                 --                                           \
                                 src/0*                                       \
                              || retval=1


echo "running CernVM-FS migration test cases..."
CVMFS_TEST_CLASS_NAME=MigrationTests                                              \
./run.sh $MIGRATIONTEST_LOGFILE -o ${MIGRATIONTEST_LOGFILE}${XUNIT_OUTPUT_SUFFIX} \
                                   migration_tests/*                              \
                                || retval=1

exit $retval
