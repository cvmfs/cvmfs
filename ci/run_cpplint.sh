#!/bin/sh

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

# define locations and file extensions of source files
SOURCE_DIRS="cvmfs mount test/unittests"

################################################################################

cd $REPO_ROOT
FILE_LIST="$(find $SOURCE_DIRS -type f -not -name '\._*' -and \( -name '*.h' -or -name '*.cc' -or -name '*.hpp' -or -name '*.c' \))"
python $CPPLINT ${FILE_LIST} 2>&1 | tee ${SCRIPT_OUTPUT}

