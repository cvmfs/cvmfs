#!/bin/sh

#
# This script wraps the unit test run of CernVM-FS.
#

set -e

SCRIPT_LOCATION=$(cd "$(dirname "$0")"; pwd)
. ${SCRIPT_LOCATION}/common.sh

usage() {
  echo "Usage: $0 [-q only quick tests] [-l preload library path] <unittests binary> <XML output location>"
  echo "This script runs the CernVM-FS unit tests"
  exit 1
}

CVMFS_UNITTESTS_QUICK=0
CVMFS_LIBRARY_PATH=

while getopts "ql:" option; do
  case $option in
    q)
      CVMFS_UNITTESTS_QUICK=1
    ;;
    l)
      CVMFS_LIBRARY_PATH=$OPTARG
    ;;
    ?)
      usage
    ;;
  esac
done

shift $(( $OPTIND - 1 ))
if [ $# -lt 2 ]; then
  usage
fi

CVMFS_UNITTESTS_BINARY=$1
CVMFS_UNITTESTS_RESULT_LOCATION=$2

# configure manual library path if needed
if [ ! -z $CVMFS_LIBRARY_PATH ]; then
  echo "using custom library path: '$CVMFS_LIBRARY_PATH'"
  if is_linux; then
    export LD_LIBRARY_PATH="$CVMFS_LIBRARY_PATH"
  elif is_macos; then
    export DYLD_LIBRARY_PATH="$CVMFS_LIBRARY_PATH"
  else
    die "who am i on? $(uname -a)"
  fi
fi

# check if only a quick subset of the unittests should be run
test_filter='-'
if [ $CVMFS_UNITTESTS_QUICK = 1 ]; then
  echo "running only quick tests (without suffix 'Slow')"
  test_filter='-*Slow'
fi

# run the unit tests
echo "running unit tests (with XML output $CVMFS_UNITTESTS_RESULT_LOCATION)..."
$CVMFS_UNITTESTS_BINARY --gtest_shuffle                                     \
                        --gtest_output=xml:$CVMFS_UNITTESTS_RESULT_LOCATION \
                        --gtest_filter=$test_filter
