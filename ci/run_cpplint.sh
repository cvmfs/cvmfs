#!/bin/sh

#
# This script runs the CppLint program and checks the general code style
#

set -e

SCRIPT_LOCATION=$(cd "$(dirname "$0")"; pwd)
. ${SCRIPT_LOCATION}/common.sh

REPO_ROOT="$(get_repository_root)"
CPPLINT="${REPO_ROOT}/cpplint.py"

[ -d $REPO_ROOT ] || die "$REPO_ROOT is malformed"
[ -f $CPPLINT ]   || die "$CPPLINT missing"

# define locations and file extensions of source files
SOURCE_DIRS="cvmfs mount test/unittests"
SOURCE_EXTS="h hpp cc cpp c"

################################################################################

cd $REPO_ROOT
EXTS_REGEX=".*\.\($(echo $SOURCE_EXTS | sed -e 's/\s\+/\\\|/g')\)\$"
python $CPPLINT $(find $SOURCE_DIRS -type f | grep -e "$EXTS_REGEX")
