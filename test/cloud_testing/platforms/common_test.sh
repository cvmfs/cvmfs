#!/bin/sh

#
# Common functionality for cloud platform test execution engine (test session)
# After sourcing this file the following variables are set:
#
#    SOURCE_DIRECTORY   location of the CernVM-FS sources forming above packages
#    TEST_LOGFILE       location of the test logfile to be used
#

SOURCE_DIRECTORY=""
TEST_LOGFILE=""

# parse script parameters (same for all platforms)
while getopts "t:l:" option; do
  case $option in
    t)
      SOURCE_DIRECTORY=$OPTARG
      ;;
    l)
      TEST_LOGFILE=$OPTARG
      ;;
    ?)
      shift $(($OPTIND-2))
      usage "Unrecognized option: $1"
      ;;
  esac
done

# check that all mandatory parameters are set
if [ x$SOURCE_DIRECTORY = "x" ] ||
   [ x$TEST_LOGFILE     = "x" ]; then
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
