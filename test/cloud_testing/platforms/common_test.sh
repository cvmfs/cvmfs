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
#    DEVEL_PACKAGE         location of the CernVM-FS devel package to test
#    LOG_DIRECTORY         location of the test log files to be created
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
DEVEL_PACKAGE=""
LOG_DIRECTORY=""

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
  echo " -l <test log directory>    destination for log file generation"

  exit 1
}


# parse script parameters (same for all platforms)
while getopts "t:s:c:d:l:" option; do
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
    ?)
      shift $(($OPTIND-2))
      usage "Unrecognized option: $1"
      ;;
  esac
done

# check that all mandatory parameters are set
if [ x$SOURCE_DIRECTORY      = "x" ] ||
   [ x$LOG_DIRECTORY         = "x" ] ||
   [ x$SERVER_PACKAGE        = "x" ] ||
   [ x$CLIENT_PACKAGE        = "x" ] ||
   [ x$DEVEL_PACKAGE         = "x" ]; then
  echo "missing parameter(s), cannot run platform dependent test script"
  exit 100
fi

sudo tee /etc/cvmfs/cvmfs_server_hooks.sh << EOF
# download GeoIP database from a copy at CERN instead of directly from MaxMind
CVMFS_UPDATEGEO_URLBASE="https://ecsft.cern.ch/dist/cvmfs/geodb"
CVMFS_UPDATEGEO_URLBASE6="https://ecsft.cern.ch/dist/cvmfs/geodb/GeoLiteCityv6-beta"
EOF

CLIENT_TEST_LOGFILE="${LOG_DIRECTORY}/test_client.log"
SERVER_TEST_LOGFILE="${LOG_DIRECTORY}/test_server.log"
TEST_S3_LOGFILE="${LOG_DIRECTORY}/test_s3.log"
FAKE_S3_LOGFILE="${LOG_DIRECTORY}/fake_s3.log"
UNITTEST_LOGFILE="${LOG_DIRECTORY}/unittest.log"
MIGRATIONTEST_LOGFILE="${LOG_DIRECTORY}/migrationtest.log"

XUNIT_OUTPUT_SUFFIX=".xunit.xml"

# check that the script is running under the correct user account
if [ $(id --user --name) != "sftnight" ]; then
  echo "test cases need to run under user 'sftnight'... aborting"
  exit 3
fi
