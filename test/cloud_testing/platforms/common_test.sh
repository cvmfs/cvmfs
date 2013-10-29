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
#    UNITTEST_LOGFILE      location of the unit test logfile to be used
#    MIGRATIONTEST_LOGFILE location of the migration test logfile to be used
#

SOURCE_DIRECTORY=""
SERVER_PACKAGE=""
CLIENT_PACKAGE=""
TEST_LOGFILE=""
UNITTEST_LOGFILE=""
MIGRATIONTEST_LOGFILE=""

usage() {
  local msg=$1

  echo "$msg"
  echo
  echo "Mandatory options:"
  echo " -t <cvmfs source tree>     CernVM-FS source tree location"
  echo " -s <cvmfs server package>  CernVM-FS server package to be tested"
  echo " -c <cvmfs client package>  CernVM-FS client package to be tested"
  echo " -l <test logfile>          logfile to write test results into"
  echo " -u <unittest logfile>      logfile to write unittest outputs into"
  echo " -m <migrationtest logfile> logfile to write migration test outputs"

  exit 1
}


# parse script parameters (same for all platforms)
while getopts "t:s:c:l:u:m:" option; do
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
