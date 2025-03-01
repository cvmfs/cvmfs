
export CVMFS_PLATFORM_NAME="debian$(. /etc/os-release && echo "$VERSION_ID")-$(uname -m)"
export CVMFS_TIMESTAMP=$(date -u +'%Y-%m-%dT%H:%M:%SZ')

# source the common platform independent functionality and option parsing
script_location=$(cd "$(dirname "$0")"; pwd)
. ${script_location}/common_test.sh

retval=0

echo "running unittests"
run_unittests --gtest_shuffle \
             --gtest_death_test_use_fork || retval=1


CVMFS_EXCLUDE=

debian_release="$(lsb_release -cs)"
CVMFS_EXCLUDE=

# Kernel sources too old for gcc, TODO
CVMFS_EXCLUDE="$CVMFS_EXCLUDE src/006-buildkernel"
# Expected failure, see test case
CVMFS_EXCLUDE="$CVMFS_EXCLUDE src/628-pythonwrappedcvmfsserver"

# Hardlinks do not work with overlayfs
CVMFS_EXCLUDE="$CVMFS_EXCLUDE src/672-publish_stats_hardlinks"

export CVMFS_TEST_UNIONFS=overlayfs

cd ${SOURCE_DIRECTORY}/test
echo "running CernVM-FS client test cases..."
CVMFS_TEST_CLASS_NAME=ClientIntegrationTests                                  \
./run.sh $CLIENT_TEST_LOGFILE -o ${CLIENT_TEST_LOGFILE}${XUNIT_OUTPUT_SUFFIX} \
                              -x src/004-davinci                              \
                                 src/005-asetup                               \
                                 src/007-testjobs                             \
                                 src/024-reload-during-asetup                 \
                                 src/094-attachmount                          \
                                 src/095-fuser                                \
                                 $CVMFS_EXCLUDE                               \
                                 --                                           \
                                 src/0*                                       \
                                 src/1*                                       \
                              || retval=1


if [ x"$(uname -m)" = x"x86_64" ]; then
  echo "running CernVM-FS server test cases..."
  CVMFS_TEST_CLASS_NAME=ServerIntegrationTests                                  \
  ./run.sh $SERVER_TEST_LOGFILE -o ${SERVER_TEST_LOGFILE}${XUNIT_OUTPUT_SUFFIX} \
                                -x src/518-hardlinkstresstest                   \
                                   src/600-securecvmfs                          \
                                   src/647-bearercvmfs                          \
                                   src/673-acl                                  \
                                   src/684-https_s3                             \
                                   src/686-azureblob_s3                         \
                                   src/687-import_s3                            \
                                   src/692-https_azureblob_s3                   \
                                   src/702-symlink_caching                      \
                                   $CVMFS_EXCLUDE                               \
                                   --                                           \
                                   src/5*                                       \
                                   src/6*                                       \
                                   src/7*                                       \
                                   src/8*                                       \
                                   src/9*                                       \
                                || retval=1
fi


echo "running CernVM-FS client migration test cases..."
CVMFS_TEST_CLASS_NAME=ClientMigrationTests                        \
./run.sh $MIGRATIONTEST_CLIENT_LOGFILE                            \
         -o ${MIGRATIONTEST_CLIENT_LOGFILE}${XUNIT_OUTPUT_SUFFIX} \
            migration_tests/0*                                    \
          || retval=1

if [ x"$(uname -m)" = x"x86_64" ]; then
  echo "running CernVM-FS server migration test cases..."
  CVMFS_TEST_CLASS_NAME=ServerMigrationTests                       \
  ./run.sh $MIGRATIONTEST_SERVER_LOGFILE                           \
          -o ${MIGRATIONTEST_SERVER_LOGFILE}${XUNIT_OUTPUT_SUFFIX} \
              migration_tests/5*                                   \
          || retval=1
fi

exit $retval
