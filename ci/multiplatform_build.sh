#!/bin/sh

#
# This script builds CernVM-FS from scratch or incrementally on as many
# platforms as possible. It can be used both by continuous integration systems
# and users.
#

if [ $# -lt 1 ]; then
  echo "Usage: $0 <CernVM-FS source directory> [<number of CPU cores>]"
  echo "This script builds CernVM-FS in its current working."
  exit 1
fi

CVMFS_SOURCE_LOCATION="$1"
CVMFS_CONCURRENT_BUILD_JOBS="${2-1}"

echo "configuring using CMake..."
cmake -DBUILD_SERVER=yes       \
      -DBUILD_SERVER_DEBUG=yes \
      -DBUILD_UNITTESTS=yes    \
      $CVMFS_SOURCE_LOCATION

echo "building using make ($CVMFS_CONCURRENT_BUILD_JOBS concurrent jobs)..."
make -j $CVMFS_CONCURRENT_BUILD_JOBS
