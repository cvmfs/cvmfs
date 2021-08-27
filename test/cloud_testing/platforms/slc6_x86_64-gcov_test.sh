#!/bin/sh

export CVMFS_PLATFORM_NAME="slc6-x86_64_gcov"
export CVMFS_TIMESTAMP=$(date -u +'%Y-%m-%dT%H:%M:%SZ')

# source the common platform independent functionality and option parsing
script_location=$(cd "$(dirname "$0")"; pwd)
. ${script_location}/common.sh

SOURCE_DIRECTORY=""
LOG_DIRECTORY=""

while getopts "t:l:" option; do
  case $option in
    t)
      SOURCE_DIRECTORY=$OPTARG
      ;;
    l)
      LOG_DIRECTORY=$OPTARG
      ;;
    ?)
      shift $(($OPTIND-2))
      usage "Unrecognized option: $1"
      ;;
  esac
done

CLIENT_TEST_LOGFILE="${LOG_DIRECTORY}/test_client.log"
SERVER_TEST_LOGFILE="${LOG_DIRECTORY}/test_server.log"
S3_TEST_LOGFILE="${LOG_DIRECTORY}/test_s3.log"
TEST_S3_LOGFILE="${LOG_DIRECTORY}/test_s3_instance.log"

XUNIT_OUTPUT_SUFFIX=".xunit.xml"

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

cd ${SOURCE_DIRECTORY}/test
echo "running CernVM-FS client test cases..."
CVMFS_TEST_CLASS_NAME=ClientIntegrationTests                                  \
./run.sh $CLIENT_TEST_LOGFILE -o ${CLIENT_TEST_LOGFILE}${XUNIT_OUTPUT_SUFFIX} \
                              -x src/004-davinci                              \
                                 src/005-asetup                               \
                                 src/006-buildkernel                          \
                                 src/024-reload-during-asetup                 \
                                 src/084-premounted                           \
                                 --                                           \
                                 src/0*                                       \
                              || retval=1


echo "running CernVM-FS server test cases..."
CVMFS_TEST_CLASS_NAME=ServerIntegrationTests                                  \
./run.sh $SERVER_TEST_LOGFILE -o ${SERVER_TEST_LOGFILE}${XUNIT_OUTPUT_SUFFIX} \
                              -x src/518-hardlinkstresstest                   \
                                 src/525-bigrepo                              \
                                 src/577-garbagecollecthiddenstratum1revision \
                                 src/579-garbagecollectstratum1legacytag      \
                                 --                                           \
                                 src/5*                                       \
                              || retval=1


# echo -n "starting the test S3 provider... "
# s3_retval=0
# test_s3_pid=$(start_test_s3 $TEST_S3_LOGFILE) || { s3_retval=1; retval=1; echo "fail"; }
# echo "done ($test_s3_pid)"

# if [ $s3_retval -eq 0 ]; then
#   echo "running CernVM-FS server test cases against the test S3 provider..."
#   CVMFS_TEST_S3_CONFIG=$TEST_S3_CONFIG                                      \
#   CVMFS_TEST_HTTP_BASE=$TEST_S3_URL                                         \
#   CVMFS_TEST_CLASS_NAME=S3ServerIntegrationTests                            \
#   ./run.sh $S3_TEST_LOGFILE -o ${S3_TEST_LOGFILE}${XUNIT_OUTPUT_SUFFIX}     \
#                             -x src/518-hardlinkstresstest                   \
#                                src/519-importlegacyrepo                     \
#                                src/522-missingchunkfailover                 \
#                                src/525-bigrepo                              \
#                                src/528-recreatespoolarea                    \
#                                src/530-recreatespoolarea_defaultkey         \
#                                src/537-symlinkedbackend                     \
#                                src/538-symlinkedstratum1backend             \
#                                src/542-storagescrubbing                     \
#                                src/543-storagescrubbing_scriptable          \
#                                src/550-livemigration                        \
#                                src/563-garbagecollectlegacy                 \
#                                src/568-migratecorruptrepo                   \
#                                src/571-localbackendumask                    \
#                                src/572-proxyfailover                        \
#                                src/577-garbagecollecthiddenstratum1revision \
#                                src/579-garbagecollectstratum1legacytag      \
#                                src/583-httpredirects                        \
#                                --                                           \
#                                src/5* || retval=1

#   echo -n "Stopping the test S3 provider... "
#   sudo kill -2 $test_s3_pid && echo "done" || echo "fail"
# fi

exit $retval
