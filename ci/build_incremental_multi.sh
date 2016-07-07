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

cd ${CVMFS_BUILD_LOCATION}

# figure out if the standard compiler is too old and if so look for alternatives
if [ x"$(uname)" = x"Linux" ] && which gcc > /dev/null 2>&1; then
  gcc_major="$(gcc --version | head -n1 | sed -e 's/^.* \([0-9]\)\..*$/\1/')"
  if [ $gcc_major -lt 4 ]; then
    if ! which gcc4 > /dev/null 2>&1 || \
       ! which g++4 > /dev/null; then
      echo "we need at least GCC 4.1"
      exit 2
    fi

    echo "using gcc4 and g++4 without optimisation as a fallback!"
    export CC=gcc4
    export CXX=g++4
    export CFLAGS="$CFLAGS -O0"     # disabling any optimisation since it likely
    export CXXFLAGS="$CXXFLAGS -O0" # breaks the binary with this old version
  fi
fi

# if we are on Mac OS X switch off both the server and the libcvmfs builds
build_server="yes"
build_libcvmfs="yes"
if [ x"$(uname)" = x"Darwin" ]; then
  build_server="no"
  build_libcvmfs="no"
fi

echo "configuring using CMake..."
cmake -DBUILD_SERVER=$build_server       \
      -DBUILD_SERVER_DEBUG=$build_server \
      -DBUILD_UNITTESTS=yes              \
      -DBUILD_LIBCVMFS=$build_libcvmfs   \
      -DBUILD_PRELOAD=yes                \
      $CVMFS_SOURCE_LOCATION

echo "building using make ($CVMFS_CONCURRENT_BUILD_JOBS concurrent jobs)..."
make -j $CVMFS_CONCURRENT_BUILD_JOBS
