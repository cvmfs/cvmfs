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

# if we are on Mac OS X switch off the server build
build_server="yes"
if [ x"$(uname)" = x"Darwin" ]; then
  build_server="no"
fi
build_libcvmfs="yes"
build_shrinkwrap="yes"

# We need to disable GEOAPI on CentOS < 6
build_geoapi="ON"
if is_redhat && [ "$(get_redhat_version)" -lt "6" ]; then
  build_geoapi="OFF"
fi

# the function can_build_ducc is defined in ci/common.sh
# it check a new enough version of the compiler >= 1.11.4
# that go-junit-report is installed
# and that we are on a 64bit architecture
build_ducc="OFF"
if [ $(can_build_ducc) -ge 1 ]; then
  build_ducc="ON"
fi

build_gateway="OFF"
if [ $(can_build_gateway) -ge 1 ]; then
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
