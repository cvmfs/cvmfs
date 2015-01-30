#!/bin/sh

# source the common platform independent functionality and option parsing
script_location=$(dirname $(readlink --canonicalize $0))
. ${script_location}/common_test.sh

ut_retval=0
it_retval=0
s3_retval=0
mg_retval=0

# format additional disks with ext4 and many inodes
echo -n "formatting new disk partitions... "
disk_to_partition=/dev/vda
partition_2=$(get_last_partition_number $disk_to_partition)
partition_1=$(( $partition_2 - 1 ))
format_partition_ext4 $disk_to_partition$partition_1 || die "fail (formatting partition 1)"
format_partition_ext4 $disk_to_partition$partition_2 || die "fail (formatting partition 2)"
echo "done"

# mount additional disk partitions on strategic cvmfs location
echo -n "mounting new disk partitions into cvmfs specific locations... "
mount_partition $disk_to_partition$partition_1 /srv             || die "fail (mounting /srv $?)"
mount_partition $disk_to_partition$partition_2 /var/spool/cvmfs || die "fail (mounting /var/spool/cvmfs $?)"
echo "done"

# allow apache access to the mounted server file system
echo -n "setting SELinux labels for apache... "
sudo chcon -Rv --type=httpd_sys_content_t /srv > /dev/null || die "fail"
echo "done"

# start apache
echo -n "starting apache... "
sudo service httpd start > /dev/null 2>&1 || die "fail"
echo "OK"

# create the server's client cache directory in /srv (AUFS kernel deadlock workaround)
echo -n "creating client caches on extra partition... "
sudo mkdir -p /srv/cache/server || die "fail (cache for server test cases)"
sudo mkdir -p /srv/cache/client || die "fail (cache for client test cases)"
echo "done"

echo -n "bind mount client cache to /var/lib/cvmfs... "
if [ ! -d /var/lib/cvmfs ]; then
  sudo rm -fR   /var/lib/cvmfs || true
  sudo mkdir -p /var/lib/cvmfs || die "fail (mkdir /var/lib/cvmfs)"
fi
sudo mount --bind /srv/cache/client /var/lib/cvmfs || die "fail (cannot bind mount /var/lib/cvmfs)"
echo "done"

# reset SELinux context
echo -n "restoring SELinux context for /var/lib/cvmfs... "
sudo restorecon -R /var/lib/cvmfs || die "fail"
echo "done"

# running unit test suite
run_unittests --gtest_shuffle \
              --gtest_death_test_use_fork || ut_retval=$?

echo "running CernVM-FS test cases..."
cd ${SOURCE_DIRECTORY}/test
export CVMFS_TEST_SERVER_CACHE='/srv/cache' &&                         \
./run.sh $TEST_LOGFILE -x src/005-asetup                               \
                          src/024-reload-during-asetup                 \
                          src/518-hardlinkstresstest                   \
                          src/523-corruptchunkfailover                 \
                          src/524-corruptmanifestfailover              \
                          src/577-garbagecollecthiddenstratum1revision \
                          src/579-garbagecollectstratum1legacytag || it_retval=$?

echo -n "starting FakeS3 service... "
fakes3_pid=$(start_fakes3 $FAKE_S3_LOGFILE) || { s3_retval=$?; die "fail"; }
echo "done ($fakes3_pid)"

if [ $s3_retval -eq 0 ]; then
    echo "running CernVM-FS server test cases against FakeS3..."
    export CVMFS_TEST_S3_CONFIG=$FAKE_S3_CONFIG                    &&         \
    export CVMFS_TEST_STRATUM0=$FAKE_S3_URL                        &&         \
    export CVMFS_TEST_SERVER_CACHE='/srv/cache'                    &&         \
    ./run.sh $TEST_S3_LOGFILE -x src/0*                                       \
                                 src/518-hardlinkstresstest                   \
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
                                 src/577-garbagecollecthiddenstratum1revision \
                                 src/579-garbagecollectstratum1legacytag      \
                                 src/583-httpredirects || s3_retval=$?

    echo -n "killing FakeS3... "
    sudo kill -2 $fakes3_pid && echo "done" || echo "fail"
fi

echo "running CernVM-FS migration test cases..."
./run.sh $MIGRATIONTEST_LOGFILE migration_tests/001-hotpatch || mg_retval=$?

[ $ut_retval -eq 0 ] && [ $it_retval -eq 0 ] && [ $s3_retval -eq 0 ] && [ $mg_retval -eq 0 ]
