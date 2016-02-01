#!/bin/bash

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

# running unittests
run_unittests --gtest_shuffle \
              --gtest_death_test_use_fork || retval=1


cd ${SOURCE_DIRECTORY}/test
echo "running CernVM-FS client test cases..."
CVMFS_TEST_CLASS_NAME=ClientIntegrationTests                                  \
./run.sh $CLIENT_TEST_LOGFILE -o ${CLIENT_TEST_LOGFILE}${XUNIT_OUTPUT_SUFFIX} \
                              -x src/004-davinci                              \
                                 src/005-asetup                               \
                                 src/007-testjobs                             \
                                 src/024-reload-during-asetup                 \
                                 --                                           \
                                 src/0*                                       \
                              || retval=1


echo "running CernVM-FS server test cases..."
CVMFS_TEST_SERVER_CACHE="$custom_cache_dir"                                   \
CVMFS_TEST_CLASS_NAME=ServerIntegrationTests                                  \
./run.sh $SERVER_TEST_LOGFILE -o ${SERVER_TEST_LOGFILE}${XUNIT_OUTPUT_SUFFIX} \
                              -x src/518-hardlinkstresstest                   \
                                 src/523-corruptchunkfailover                 \
                                 src/524-corruptmanifestfailover              \
                                 src/577-garbagecollecthiddenstratum1revision \
                                 src/579-garbagecollectstratum1legacytag      \
                                 src/585-xattrs                               \
                                 src/600-securecvmfs                          \
                                 --                                           \
                                 src/5*                                       \
                                 src/6*                                       \
                              || retval=1


echo "running CernVM-FS migration test cases..."
CVMFS_TEST_CLASS_NAME=MigrationTests \
./run.sh $MIGRATIONTEST_LOGFILE -o ${MIGRATIONTEST_LOGFILE}${XUNIT_OUTPUT_SUFFIX} \
                                   migration_tests/*                              \
                                || retval=1

exit $retval
