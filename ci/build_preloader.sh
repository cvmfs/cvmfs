#!/bin/bash

#
# This script builds the doxygen documentation
#

set -e

SCRIPT_LOCATION=$(cd "$(dirname "$0")"; pwd)
. ${SCRIPT_LOCATION}/common.sh

BUILD_DIR=
if [ "x$1" != "x" ]; then
  BUILD_DIR="$1"
else
  die "build directory missing"
fi

REPO_ROOT="$(get_repository_root)"

[ -d $REPO_ROOT ] || die "$REPO_ROOT is malformed"

echo "Build directory: $BUILD_DIR"
echo "Source directory: $REPO_ROOT"

cd $BUILD_DIR
cmake -DBUILD_PRELOADER=on \
  -DBUILD_CVMFS=no \
  -DBUILD_SERVER=no \
  -DBUILD_LIBCVMFS=no \
  -DBUILD_LIBCVMFS_CACHE=no \
  -DBUILD_SHRINKWRAP=no \
  $REPO_ROOT
make -j $(nproc)
