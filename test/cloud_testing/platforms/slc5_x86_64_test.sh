#!/bin/sh

# source the common platform independent functionality and option parsing
script_location=$(dirname $(readlink --canonicalize $0))
. ${script_location}/common_test.sh

ut_retval=0
it_retval=0
mg_retval=0

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
echo -n "creating client cache on extra partition... "
sudo mkdir /srv/cache || die "fail"
echo "done"

# running unit test suite
run_unittests --gtest_shuffle || ut_retval=$?

echo "running CernVM-FS test cases..."
cd ${SOURCE_DIRECTORY}/test
export CVMFS_TEST_SERVER_CACHE='/srv/cache' &&         \
./run.sh $TEST_LOGFILE -x src/004-davinci              \
                          src/007-testjobs             \
                          src/045-oasis                \
                          src/518-hardlinkstresstest   \
                          src/523-corruptchunkfailover \
                          src/524-corruptmanifestfailover || it_retval=$?

echo "running CernVM-FS migration test cases..."
./run.sh $MIGRATIONTEST_LOGFILE migration_tests/001-hotpatch || mg_retval=$?

[ $ut_retval -eq 0 ] && [ $it_retval -eq 0 ] && [ $mg_retval -eq 0 ]
