#!/bin/sh

# source the common platform independent functionality and option parsing
script_location=$(cd "$(dirname "$0")"; pwd)
. ${script_location}/common_test.sh

retval=0

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

# create the server's client cache directory in /srv (EXDEV kernel deadlock workaround)
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
