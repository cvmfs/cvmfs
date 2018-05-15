#!/bin/sh

# source the common platform independent functionality and option parsing
script_location=$(cd "$(dirname "$0")"; pwd)
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

retval=0

# running unit test suite
run_unittests --gtest_shuffle || retval=1

cd ${SOURCE_DIRECTORY}/test
echo "running CernVM-FS client test cases..."
CVMFS_TEST_CLASS_NAME=ClientIntegrationTests                                  \
./run.sh $CLIENT_TEST_LOGFILE -o ${CLIENT_TEST_LOGFILE}${XUNIT_OUTPUT_SUFFIX} \
                              -x src/004-davinci                              \
                                 src/005-asetup                               \
                                 src/007-testjobs                             \
                                 src/024-reload-during-asetup                 \
                                 src/045-oasis                                \
                                 src/079-dnsroaming                           \
                                 --                                           \
                                 src/0*                                       \
                              || retval=1

echo "running CernVM-FS migration test cases..."
CVMFS_TEST_CLASS_NAME=MigrationTests                                              \
./run.sh $MIGRATIONTEST_LOGFILE -o ${MIGRATIONTEST_LOGFILE}${XUNIT_OUTPUT_SUFFIX} \
                                   migration_tests/*                              \
                                || retval=1

exit $retval
