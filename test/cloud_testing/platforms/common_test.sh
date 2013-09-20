#!/bin/sh

#
# Common functionality for cloud platform test execution engine (test session)
# After sourcing this file the following variables are set:
#
#    SOURCE_DIRECTORY   location of the CernVM-FS sources forming above packages
#    SERVER_PACKAGE     location of the CernVM-FS server package to test
#    CLIENT_PACKAGE     location of the CernVM-FS client package to test
#    TEST_LOGFILE       location of the test logfile to be used
#    UNITTEST_LOGFILE   location of the unit test logfile to be used
#

SOURCE_DIRECTORY=""
SERVER_PACKAGE=""
CLIENT_PACKAGE=""
TEST_LOGFILE=""
UNITTEST_LOGFILE=""

# parse script parameters (same for all platforms)
while getopts "t:s:c:l:u:" option; do
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
    ?)
      shift $(($OPTIND-2))
      usage "Unrecognized option: $1"
      ;;
  esac
done

# check that all mandatory parameters are set
if [ x$SOURCE_DIRECTORY = "x" ] ||
   [ x$TEST_LOGFILE     = "x" ] ||
   [ x$UNITTEST_LOGFILE = "x" ] ||
   [ x$SERVER_PACKAGE   = "x" ] ||
   [ x$CLIENT_PACKAGE   = "x" ]; then
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


die() {
  local msg="$1"
  echo $msg
  exit 103
}


run_unittests() {
  echo -n "running CernVM-FS unit tests... "
  cvmfs_unittests $@ >> $UNITTEST_LOGFILE 2>&1

  local ut_retval=$?
  if [ $ut_retval -ne 0 ]; then
    echo "Failed!"
  else
    echo "OK"
  fi

  return $ut_retval
}
