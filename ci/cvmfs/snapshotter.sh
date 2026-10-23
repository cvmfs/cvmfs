#!/bin/sh

#
# This script builds the CernVM-FS snapshotter.
#

set -e

SCRIPT_LOCATION=$(cd "$(dirname "$0")"; pwd)
. ${SCRIPT_LOCATION}/../common.sh

if [ $# -lt 2 ]; then
  echo "Usage: $0 <CernVM-FS source directory> <build result location> [<nightly build number>]"
  echo "This script builds CernVM-FS snapshotter container"
  exit 1
fi

CVMFS_SOURCE_LOCATION="$1"
CVMFS_RESULT_LOCATION="$2"
CVMFS_NIGHTLY_BUILD_NUMBER="${3-0}"

# Preserve the legacy nightly-number interface.
set_cvmfs_version_sequence_from_nightly "$CVMFS_NIGHTLY_BUILD_NUMBER"

cvmfs_version="$(get_cvmfs_version "$CVMFS_SOURCE_LOCATION")"
cvmfs_tag_version=$(echo "$cvmfs_version" | tr '~+' '--')
echo "detected upstream version: $cvmfs_version"

CVMFS_TAG="${cvmfs_tag_version}-1"
echo "creating container: $CVMFS_TAG"

if [ -d ${CVMFS_RESULT_LOCATION}/build ]; then
  if [ ! -z "$(ls -A ${CVMFS_RESULT_LOCATION}/build)" ]; then
    echo "The /build directory should be empty"
    exit 1
  fi
else
  mkdir -p ${CVMFS_RESULT_LOCATION}/build
fi

# build commands
cd ${CVMFS_RESULT_LOCATION}/build
cmake -DBUILD_SNAPSHOTTER=yes -DBUILD_CVMFS=no \
  -DBUILD_SERVER=no -DBUILD_RECEIVER=no -DBUILD_GEOAPI=no \
  -DBUILD_LIBCVMFS=no -DBUILD_LIBCVMFS_CACHE=no \
  -DINSTALL_BASH_COMPLETION=no \
  -DEXTERNALS_PREFIX=${CVMFS_RESULT_LOCATION}/externals \
  ${CVMFS_SOURCE_LOCATION}
make -j4

mv ${CVMFS_RESULT_LOCATION}/build/snapshotter/cvmfs_snapshotter ${CVMFS_RESULT_LOCATION}/build/snapshotter/cvmfs_snapshotter.${CVMFS_TAG}.$(uname -m)
