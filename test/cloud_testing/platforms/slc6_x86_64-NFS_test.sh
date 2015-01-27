#!/bin/sh

# source the common platform independent functionality and option parsing
script_location=$(dirname $(readlink --canonicalize $0))
. ${script_location}/common_test.sh

# format additional disks with ext4 and many inodes
echo -n "formatting new disk partition... "
disk_to_partition=/dev/vda
partition=$(get_last_partition_number $disk_to_partition)
format_partition_ext4 $disk_to_partition$partition || die "fail (formatting partition)"
echo "done"

# mount additional disk partitions on strategic cvmfs location
echo -n "mounting new disk partition into cvmfs specific location... "
mount_partition $disk_to_partition$partition /var/lib/cvmfs || die "fail (mounting /var/lib/cvmfs $?)"
echo "done"

ut_retval=0
it_retval=0
mg_retval=0

# run tests
echo "running CernVM-FS test cases..."
cd ${SOURCE_DIRECTORY}/test
./run.sh $TEST_LOGFILE -x src/001-chksetup             \
                          src/005-asetup               \
                          src/008-default_domain       \
                          src/024-reload-during-asetup \
                          src/026-tightcache           \
                          src/041-rocache              \
                          src/043-highinodes           \
                          src/5* || it_retval=$?

echo "running CernVM-FS migration test cases..."
./run.sh $MIGRATIONTEST_LOGFILE migration_tests/001-hotpatch || mg_retval=$?

[ $ut_retval -eq 0 ] && [ $it_retval -eq 0 ] && [ $mg_retval -eq 0 ]
