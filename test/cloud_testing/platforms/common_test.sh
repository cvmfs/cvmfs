#!/bin/sh

script_location=$(dirname $(readlink --canonicalize $0))
. ${script_location}/common.sh

#
# Common functionality for cloud platform test execution engine (test session)
# After sourcing this file the following variables are set:
#
#    SOURCE_DIRECTORY      location of the CernVM-FS sources
#    SERVER_PACKAGE        location of the CernVM-FS server package to test
#    CLIENT_PACKAGE        location of the CernVM-FS client package to test
#    TEST_LOGFILE          location of the test logfile to be used
#    TEST_S3_LOGFILE       location of the test logfile for S3 tests to be used
#    FAKE_S3_LOGFILE       location of the FakeS3 server's output to be used
#    UNITTEST_LOGFILE      location of the unit test logfile to be used
#    MIGRATIONTEST_LOGFILE location of the migration test logfile to be used
#
# Additionally the following configuration variables will be defined:
#    FAKE_S3_PORT          network port to communicate with FakeS3
#    FAKE_S3_STORAGE       storage location of FakeS3
#    FAKE_S3_CONFIG        location of the S3 config file to be created/used
#    FAKE_S3_BUCKET        name of the S3 bucket to be used
#    FAKE_S3_URL           URL to the S3 server
#

SOURCE_DIRECTORY=""
SERVER_PACKAGE=""
CLIENT_PACKAGE=""
TEST_LOGFILE=""
TEST_S3_LOGFILE=""
FAKE_S3_LOGFILE=""
UNITTEST_LOGFILE=""
MIGRATIONTEST_LOGFILE=""

FAKE_S3_PORT=13337
FAKE_S3_STORAGE=/srv/fakes3
FAKE_S3_CONFIG=/etc/cvmfs/fakes3.conf
FAKE_S3_BUCKET=cvmfs_test
FAKE_S3_URL=http://localhost:${FAKE_S3_PORT}/${FAKE_S3_BUCKET}-1-1

usage() {
  local msg=$1

  echo "$msg"
  echo
  echo "Mandatory options:"
  echo " -t <cvmfs source tree>     CernVM-FS source tree location"
  echo " -s <cvmfs server package>  CernVM-FS server package to be tested"
  echo " -c <cvmfs client package>  CernVM-FS client package to be tested"
  echo " -l <test logfile>          logfile to write test results into"
  echo " -i <S3 test logfile>       logfile to write S3 server test results into"
  echo " -j <FakeS3 logfile>        logfile to write FakeS3 server output into"
  echo " -u <unittest logfile>      logfile to write unittest outputs into"
  echo " -m <migrationtest logfile> logfile to write migration test outputs"

  exit 1
}


# parse script parameters (same for all platforms)
while getopts "t:s:c:l:i:j:u:m:" option; do
  case $option in
    t)
      SOURCE_DIRECTORY=$OPTARG
      ;;
    s)
      SERVER_PACKAGE=$OPTARG
      ;;
    c)
      CLIENT_PACKAGE=$OPTARG
      ;;
    l)
      TEST_LOGFILE=$OPTARG
      ;;
    i)
      TEST_S3_LOGFILE=$OPTARG
      ;;
    j)
      FAKE_S3_LOGFILE=$OPTARG
      ;;
    u)
      UNITTEST_LOGFILE=$OPTARG
      ;;
    m)
      MIGRATIONTEST_LOGFILE=$OPTARG
      ;;
    ?)
      shift $(($OPTIND-2))
      usage "Unrecognized option: $1"
      ;;
  esac
done

# check that all mandatory parameters are set
if [ x$SOURCE_DIRECTORY      = "x" ] ||
   [ x$TEST_LOGFILE          = "x" ] ||
   [ x$TEST_S3_LOGFILE       = "x" ] ||
   [ x$FAKE_S3_LOGFILE       = "x" ] ||
   [ x$UNITTEST_LOGFILE      = "x" ] ||
   [ x$MIGRATIONTEST_LOGFILE = "x" ] ||
   [ x$SERVER_PACKAGE        = "x" ] ||
   [ x$CLIENT_PACKAGE        = "x" ]; then
  echo "missing parameter(s), cannot run platform dependent test script"
  exit 100
fi

# check that the script is running under the correct user account
if [ $(id --user --name) != "sftnight" ]; then
  echo "test cases need to run under user 'sftnight'... aborting"
  exit 3
fi

#
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
#

create_fakes3_config() {
  [ ! -f $FAKE_S3_CONFIG ] || sudo rm -f $FAKE_S3_CONFIG
  sudo tee $FAKE_S3_CONFIG > /dev/null << EOF
CVMFS_S3_HOST=localhost
CVMFS_S3_PORT=$FAKE_S3_PORT
CVMFS_S3_ACCESS_KEY=not
CVMFS_S3_SECRET_KEY=important
CVMFS_S3_BUCKETS_PER_ACCOUNT=1
CVMFS_S3_MAX_NUMBER_OF_PARALLEL_CONNECTIONS=10
CVMFS_S3_BUCKET=$FAKE_S3_BUCKET
EOF
}


start_fakes3() {
  local logfile=$1

  [ ! -d $FAKE_S3_STORAGE ] || sudo rm -fR $FAKE_S3_STORAGE > /dev/null 2>&1 || return 1
  sudo mkdir -p $FAKE_S3_STORAGE                            > /dev/null 2>&1 || return 2
  create_fakes3_config                                      > /dev/null 2>&1 || return 3
  run_background_service $logfile "sudo fakes3 --port $FAKE_S3_PORT --root $FAKE_S3_STORAGE"
}


check_result() {
  local res=$1
  if [ $res -ne 0 ]; then
    echo "Failed!"
  else
    echo "OK"
  fi
}


run_unittests() {
  echo -n "running CernVM-FS unit tests... "
  cvmfs_unittests $@ >> $UNITTEST_LOGFILE 2>&1
  local ut_retval=$?
  check_result $ut_retval

  return $ut_retval
}
