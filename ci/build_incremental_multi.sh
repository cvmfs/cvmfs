#!/bin/sh

#
# This script builds CernVM-FS from scratch or incrementally on as many
# platforms as possible. It can be used both by continuous integration systems
# and users.
#

set -e

if [ $# -lt 2 ]; then
  echo "Usage: $0 <CernVM-FS source directory> <build directory> [<number of CPU cores>]"
  echo "This script builds CernVM-FS in its current working."
  exit 1
fi

CVMFS_SOURCE_LOCATION="$1"
CVMFS_BUILD_LOCATION="$2"
CVMFS_CONCURRENT_BUILD_JOBS="${3-1}"

. ${CVMFS_SOURCE_LOCATION}/ci/common.sh

cd ${CVMFS_BUILD_LOCATION}

# if we are on Mac OS X switch off the server build
build_server="yes"
if [ x"$(uname)" = x"Darwin" ]; then
  build_server="no"
fi
build_libcvmfs="yes"
build_shrinkwrap="yes"
build_geoapi="ON"

# the function can_build_ducc is defined in ci/common.sh
# it check a new enough version of the compiler >= 1.11.4
# that go-junit-report is installed
# and that we are on a 64bit architecture
build_ducc="OFF"
if can_build_ducc; then
  build_ducc="ON"
fi

build_gateway="OFF"
if can_build_gateway; then
  build_gateway="ON"
fi

echo "configuring using CMake..."
cmake -DBUILD_SERVER=$build_server          \
      -DBUILD_SERVER_DEBUG=$build_server    \
      -DBUILD_UNITTESTS=yes                 \
      -DBUILD_LIBCVMFS=$build_libcvmfs      \
      -DBUILD_SHRINKWRAP=$build_shrinkwrap  \
      -DBUILD_GEOAPI=$build_geoapi          \
      -DBUILD_PRELOADER=yes                 \
      -DBUILD_GATEWAY=$build_gateway        \
      -DBUILD_DUCC=$build_ducc              \
      $CVMFS_SOURCE_LOCATION

echo "building using make ($CVMFS_CONCURRENT_BUILD_JOBS concurrent jobs)..."
make -j $CVMFS_CONCURRENT_BUILD_JOBS
