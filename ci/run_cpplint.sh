#!/bin/bash

#
# This script runs the CppLint program and checks the general code style
#

set -e

SCRIPT_LOCATION=$(cd "$(dirname "$0")"; pwd)
. ${SCRIPT_LOCATION}/common.sh

SCRIPT_OUTPUT=
if [ "x$1" != "x" ]; then
  SCRIPT_OUTPUT="$1"
fi

REPO_ROOT="$(get_repository_root)"
CPPLINT="${REPO_ROOT}/cpplint.py"

[ -d $REPO_ROOT ] || die "$REPO_ROOT is malformed"
[ -f $CPPLINT ]   || die "$CPPLINT missing"

set +e
PYTHON=python2
PYTHON_VERSION_STR=$($PYTHON -V 2>&1)
if [ $? -ne 0 ]; then
  PYTHON=python
fi
PYTHON_VERSION_STR=$($PYTHON -V 2>&1)
if [ $? -ne 0 ]; then
  echo "WARNING: python missing, skipping linter"
  cat /dev/null > ${SCRIPT_OUTPUT}
  exit 0
fi
set -e

PYTHON_MAJOR=$(echo $PYTHON_VERSION_STR | awk '{print $2}' | cut -d. -f1)
PYTHON_MINOR=$(echo $PYTHON_VERSION_STR | awk '{print $2}' | cut -d. -f2)
if [ $PYTHON_MAJOR -lt 2 -o $PYTHON_MAJOR -eq 2 -a $PYTHON_MINOR -lt 6 ]; then
  echo "WARNING: python too old (< 2.6), skipping linter"
  cat /dev/null > ${SCRIPT_OUTPUT}
  exit 0
fi


# define locations and file extensions of source files
SOURCE_DIRS="cvmfs mount test/unittests test/micro-benchmarks test/stress"

################################################################################

cd $REPO_ROOT
FILE_LIST="$(find $SOURCE_DIRS -type f -not -name '\._*' -and \( -name '*.h' -or -name '*.cc' -or -name '*.hpp' -or -name '*.c' \))"
$PYTHON $CPPLINT ${FILE_LIST} 2>&1 | tee ${SCRIPT_OUTPUT}
exit ${PIPESTATUS[0]}
