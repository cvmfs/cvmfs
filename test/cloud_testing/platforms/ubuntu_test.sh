

# source the common platform independent functionality and option parsing
script_location=$(cd "$(dirname "$0")"; pwd)
. ${script_location}/common_test.sh

retval=0

# running unittests
run_unittests --gtest_shuffle \
              --gtest_death_test_use_fork || retval=1


CVMFS_EXCLUDE=
if [ x"$(lsb_release -cs)" = x"xenial" ]; then
  # Ubuntu 16.04
  # Kernel sources too old for gcc, TODO
  CVMFS_EXCLUDE="src/006-buildkernel"
  # Should work once packages are built on destination platform, TODO
  CVMFS_EXCLUDE="$CVMFS_EXCLUDE src/602-libcvmfs"
  # Expected failure, see test case
  CVMFS_EXCLUDE="$CVMFS_EXCLUDE src/628-pythonwrappedcvmfsserver"

  echo "Ubuntu 16.04... using overlayfs"
  export CVMFS_TEST_UNIONFS=overlayfs
fi
if [ x"$(lsb_release -cs)" = x"trusty" ]; then
  # Ubuntu 14.04
  # aufs, expected failure
  CVMFS_EXCLUDE="src/700-overlayfs_validation src/80*-repository_gateway*"

  echo "Ubuntu 14.04... using aufs instead of overlayfs"
fi
if [ x"$(lsb_release -cs)" = x"precise" ]; then
  # Ubuntu 12.04
  # aufs, expected failure
  CVMFS_EXCLUDE="src/614-geoservice src/700-overlayfs_validation src/80*-repository_gateway*"

  echo "Ubuntu 12.04... using aufs instead of overlayfs"
fi


cd ${SOURCE_DIRECTORY}/test
echo "running CernVM-FS client test cases..."
CVMFS_TEST_CLASS_NAME=ClientIntegrationTests                                  \
./run.sh $CLIENT_TEST_LOGFILE -o ${CLIENT_TEST_LOGFILE}${XUNIT_OUTPUT_SUFFIX} \
                              -x src/004-davinci                              \
                                 src/005-asetup                               \
                                 src/007-testjobs                             \
                                 src/024-reload-during-asetup                 \
                                 src/050-configrepo                           \
                                 $CVMFS_EXCLUDE                               \
                                 --                                           \
                                 src/0*                                       \
                              || retval=1


if [ x"$(uname -m)" = x"x86_64" ]; then
  echo "running CernVM-FS server test cases..."
  CVMFS_TEST_CLASS_NAME=ServerIntegrationTests                                  \
  ./run.sh $SERVER_TEST_LOGFILE -o ${SERVER_TEST_LOGFILE}${XUNIT_OUTPUT_SUFFIX} \
                                -x src/518-hardlinkstresstest                   \
                                   src/585-xattrs                               \
                                   src/600-securecvmfs                          \
                                   $CVMFS_EXCLUDE                               \
                                   --                                           \
                                   src/5*                                       \
                                   src/6*                                       \
                                   src/7*                                       \
                                   src/8*                                       \
                                || retval=1
fi

# TODO: after the 2.4 release there will be distro specific "previous packages"
if [ x"$(lsb_release -cs)" = x"precise" ]; then
  # Ubuntu 12.04
  echo "running CernVM-FS migration test cases..."
  CVMFS_TEST_CLASS_NAME=MigrationTests \
  ./run.sh $MIGRATIONTEST_LOGFILE -o ${MIGRATIONTEST_LOGFILE}${XUNIT_OUTPUT_SUFFIX} \
                                     migration_tests/*                              \
                                  || retval=1
else
  echo "no previous client package, skipping migration test"
fi

exit $retval
