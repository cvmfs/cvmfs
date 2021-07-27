#!/bin/bash

#
# This script runs clang-tidy in the build directory.  Requires the build
# to be created with `cmake -DCMAKE_EXPORT_COMPILE_COMMANDS=ON`
#

set -e

SCRIPT_LOCATION=$(cd "$(dirname "$0")"; pwd)
. ${SCRIPT_LOCATION}/common.sh

BUILD_DIR=$1
if [ "x$BUILD_DIR" = "x" ]; then
  die "build directory missing (usage: '$0 <build dir>')"
fi
SCRIPT_OUTPUT=$2

REPO_ROOT="$(get_repository_root)"

[ -d $REPO_ROOT ] || die "$REPO_ROOT is malformed"
clang-tidy --version >/dev/null 2>&1 || die "clang-tidy missing"

SOURCE_FILES=$(find $REPO_ROOT/mount -name '*.cc')
#SOURCE_FILES="$SOURCE_FILES $REPO_ROOT/cvmfs/util/exception.cc"

if [ "x$SCRIPT_OUTPUT" != "x" ]; then
  clang-tidy -p $BUILD_DIR $SOURCE_FILES 2>&1 | tee ${SCRIPT_OUTPUT}
  exit ${PIPESTATUS[0]}
else
  clang-tidy -p $BUILD_DIR $SOURCE_FILES
fi
