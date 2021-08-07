#!/bin/bash

#
# This script runs clang-tidy in the build directory.  Requires the build
# to be created with `cmake -DCMAKE_EXPORT_COMPILE_COMMANDS=ON`
#

set -e

SCRIPT_LOCATION=$(cd "$(dirname "$0")"; pwd)
. ${SCRIPT_LOCATION}/common.sh

BUILD=0
DIFF=0
BUILD_DIR=$1
if [ "x$BUILD_DIR" = "x-build" ]; then
  BUILD=1
  shift
  BUILD_DIR=$1
fi
if [ "x$BUILD_DIR" = "x-diff" ]; then
  DIFF=1
  shift
  BUILD_DIR=$1
fi
if [ "x$BUILD_DIR" = "x" ]; then
  die "build directory missing (usage: '$0 [-build] [-diff] <build dir> <output>')"
fi
SCRIPT_OUTPUT=$2
if [ "x$SCRIPT_OUTPUT" = "x" ]; then
  die "output file missing (usage: '$0 [-build] [-diff] <build dir> <output>')"
fi

REPO_ROOT="$(get_repository_root)"

[ -d $REPO_ROOT ] || die "$REPO_ROOT is malformed"
clang-tidy --version >/dev/null 2>&1 || die "clang-tidy missing"

if [ $BUILD -eq 1 ]; then
  cd $BUILD_DIR
  cmake -DBUILD_ALL=ON -DCMAKE_EXPORT_COMPILE_COMMANDS=ON $REPO_ROOT
  make -j$(nproc)
fi

SOURCE_FILES=$(find $REPO_ROOT/mount $REPO_ROOT/cvmfs/util -name '*.cc')

cd $REPO_ROOT
if [ $DIFF -eq 0 ]; then
  CMD0="true"
  CMD1="clang-tidy -p $BUILD_DIR $SOURCE_FILES"
else
  CMD0="git diff -U0 origin/devel"
  CMD1="/usr/share/clang/clang-tidy-diff.py -j$(nproc) -path $BUILD_DIR -p1"
fi

echo "Running '$CMD0 | $CMD1'"
$CMD0 | $CMD1 2>&1 | tee ${SCRIPT_OUTPUT}
! grep -q "warning treated as error" ${SCRIPT_OUTPUT}
