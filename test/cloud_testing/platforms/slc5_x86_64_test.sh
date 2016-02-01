#!/bin/sh

# source the common platform independent functionality and option parsing
script_location=$(cd "$(dirname "$0")"; pwd)
. ${script_location}/common_test.sh

retval=0

# check $PATH variable
extend_path() {
  local path_entry=$1
  if ! echo "$PATH" | grep -q $path_entry; then
    export PATH="$PATH:$path_entry"
  fi
}
extend_path "/usr/sbin"
extend_path "/sbin"

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

# start apache
echo -n "starting apache... "
sudo service httpd start > /dev/null 2>&1 || die "fail"
echo "OK"

# loading the aufs kernel module
echo -n "activate aufs... "
kobj=$(rpm -ql $(rpm -qa | grep kernel-module-aufs) | tail -n1)
sudo /sbin/insmod $kobj || die "fail"
echo "done"

# allow httpd on backend storage
echo -n "allowing httpd to access /srv/cvmfs... "
sudo mkdir /srv/cvmfs                                      || die "fail (mkdir)"
sudo chcon -Rv --type=httpd_sys_content_t /srv > /dev/null || die "fail (chcon)"
echo "done"

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
run_unittests --gtest_shuffle || retval=1

cd ${SOURCE_DIRECTORY}/test
echo "running CernVM-FS client test cases..."
CVMFS_TEST_CLASS_NAME=ClientIntegrationTests                                  \
./run.sh $CLIENT_TEST_LOGFILE -o ${CLIENT_TEST_LOGFILE}${XUNIT_OUTPUT_SUFFIX} \
                              -x src/004-davinci                              \
                                 src/005-asetup                               \
                                 src/007-testjobs                             \
                                 src/045-oasis                                \
                                 --                                           \
                                 src/0*                                       \
                              || retval=1


echo "running CernVM-FS server test cases..."
CVMFS_TEST_SERVER_CACHE='/srv/cache/server'                                   \
CVMFS_TEST_CLASS_NAME=ServerIntegrationTests                                  \
./run.sh $SERVER_TEST_LOGFILE -o ${SERVER_TEST_LOGFILE}${XUNIT_OUTPUT_SUFFIX} \
                              -x src/518-hardlinkstresstest                   \
                                 src/523-corruptchunkfailover                 \
                                 src/524-corruptmanifestfailover              \
                                 src/577-garbagecollecthiddenstratum1revision \
                                 src/579-garbagecollectstratum1legacytag      \
                                 src/585-xattrs                               \
                                 src/600-securecvmfs                          \
                                 src/608-infofile                             \
                                 src/609-metainfofile                         \
                                 --                                           \
                                 src/5*                                       \
                                 src/6*                                       \
                              || retval=1


echo "running CernVM-FS migration test cases..."
CVMFS_TEST_CLASS_NAME=MigrationTests                                              \
./run.sh $MIGRATIONTEST_LOGFILE -o ${MIGRATIONTEST_LOGFILE}${XUNIT_OUTPUT_SUFFIX} \
                                   migration_tests/*                              \
                                || retval=1

exit $retval
