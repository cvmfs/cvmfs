#!/bin/sh

portable_dirname() {
  if [ "x$(uname -s)" = "xDarwin" ]; then
    echo $(dirname $(/usr/local/bin/greadlink --canonicalize $1))
  else
    echo $(dirname $(readlink --canonicalize $1))
  fi
}

script_location=$(portable_dirname $0)
. ${script_location}/common.sh

#
# Common functionality for cloud platform test execution engine (test session)
# After sourcing this file the following variables are set:
#
#    SOURCE_DIRECTORY      location of the CernVM-FS sources
#    SERVER_PACKAGE        location of the CernVM-FS server package to test
#    CLIENT_PACKAGE        location of the CernVM-FS client package to test
#    DEVEL_PACKAGE         location of the CernVM-FS devel package to test
#    LOG_DIRECTORY         location of the test log files to be created
#
# Additionally the following configuration variables will be defined:
#    TEST_S3_PORT          network port to communicate with the test S3 provider
#    TEST_S3_STORAGE       storage location of the test S3 provider
#    TEST_S3_CONFIG        location of the S3 config file to be created/used
#    TEST_S3_BUCKET        name of the S3 bucket to be used
#    TEST_S3_URL           URL to the S3 server
#

SOURCE_DIRECTORY=""
SERVER_PACKAGE=""
CLIENT_PACKAGE=""
DEVEL_PACKAGE=""
LOG_DIRECTORY=""

TEST_S3_PORT=13337
TEST_S3_STORAGE=/srv/test_s3
TEST_S3_CONFIG=/etc/cvmfs/test_s3.conf
TEST_S3_BUCKET=cvmfstest
TEST_S3_URL=http://localhost:${TEST_S3_PORT}/${TEST_S3_BUCKET}

CVMFS_TEST_SUITES=""

usage() {
  local msg=$1

  echo "$msg"
  echo
  echo "Mandatory options:"
  echo " -t <cvmfs source tree>     CernVM-FS source tree location"
  echo " -s <cvmfs server package>  CernVM-FS server package to be tested"
  echo " -c <cvmfs client package>  CernVM-FS client package to be tested"
  echo " -l <test log directory>    destination for log file generation"

  exit 1
}


# parse script parameters (same for all platforms)
while getopts "t:s:c:d:l:S:G:" option; do
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
    d)
      DEVEL_PACKAGE=$OPTARG
      ;;
    l)
      LOG_DIRECTORY=$OPTARG
      ;;
    S)
      CVMFS_TEST_SUITES="$OPTARG"
      ;;
    G)
      export CVMFS_TEST_GEO_LICENSE_KEY="$OPTARG"
      ;;
    ?)
      shift $(($OPTIND-2))
      usage "Unrecognized option: $1"
      ;;
  esac
done

# check that all mandatory parameters are set
if [ x$SOURCE_DIRECTORY      = "x" ] ||
   [ x$LOG_DIRECTORY         = "x" ] ||
   [ x$CLIENT_PACKAGE        = "x" ]; then
  echo "missing parameter(s), cannot run platform dependent test script"
  exit 100
fi
if [ "x$(uname -s)" != "xDarwin" ]; then
    if [ x$SERVER_PACKAGE        = "x" ] ||
       [ x$DEVEL_PACKAGE         = "x" ]; then
      echo "missing parameter(s), cannot run platform dependent test script"
      exit 200
    fi
fi

CLIENT_TEST_LOGFILE="${LOG_DIRECTORY}/${CVMFS_PLATFORM_NAME}_test_client.log"
SERVER_TEST_LOGFILE="${LOG_DIRECTORY}/${CVMFS_PLATFORM_NAME}_test_server.log"
S3_TEST_LOGFILE="${LOG_DIRECTORY}/${CVMFS_PLATFORM_NAME}_test_s3.log"
TEST_S3_LOGFILE="${LOG_DIRECTORY}/${CVMFS_PLATFORM_NAME}_test_s3_instance.log"
UNITTEST_LOGFILE="${LOG_DIRECTORY}/${CVMFS_PLATFORM_NAME}_unittest.log"
MIGRATIONTEST_CLIENT_LOGFILE="${LOG_DIRECTORY}/${CVMFS_PLATFORM_NAME}_migrationtest_client.log"
MIGRATIONTEST_SERVER_LOGFILE="${LOG_DIRECTORY}/${CVMFS_PLATFORM_NAME}_migrationtest_server.log"
DUCCTEST_LOGFILE="${LOG_DIRECTORY}/${CVMFS_PLATFORM_NAME}_test_ducc.log"

XUNIT_OUTPUT_SUFFIX=".xunit.xml"

# check that the script is running under the correct user account
if [ $(id -u -n) != "sftnight" ]; then
  echo "test cases need to run under user 'sftnight'... aborting"
  exit 3
fi

export CVMFS_TEST_SUITES
