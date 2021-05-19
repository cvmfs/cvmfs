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

# retrieve the upstream version string from CVMFS
cvmfs_version="$(get_cvmfs_version_from_cmake $CVMFS_SOURCE_LOCATION)"
echo "detected upstream version: $cvmfs_version"

git_hash="$(get_cvmfs_git_revision $CVMFS_SOURCE_LOCATION)"

# generate the release tag for either a nightly build or a release
CVMFS_TAG=
if [ $CVMFS_NIGHTLY_BUILD_NUMBER -gt 0 ]; then
  build_tag="git-${git_hash}"
  nightly_tag="0.${CVMFS_NIGHTLY_BUILD_NUMBER}.${git_hash}git"

  echo "creating nightly build '$nightly_tag'"
  CVMFS_TAG="${cvmfs_version}-$nightly_tag"
else
  echo "creating release: $cvmfs_version"
  CVMFS_TAG="${cvmfs_version}-1"
fi

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
