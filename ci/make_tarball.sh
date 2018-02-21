#!/bin/sh

set -e

SCRIPT_LOCATION=$(cd "$(dirname "$0")"; pwd)

if [ $# -lt 1 ]; then
  echo "Usage: $0 <CernVM-FS source directory (the same as the build directory)>"
  echo "This script builds packages for the current platform."
  exit 1
fi

CVMFS_BUILD_LOCATION="$1"
shift 1

export REBAR_CACHE_DIR=$CVMFS_BUILD_LOCATION/../
mkdir -p $REBAR_CACHE_DIR

# run the build script
echo "switching to $CVMFS_BUILD_LOCATION..."
cd "$CVMFS_BUILD_LOCATION"
rebar3 as prod compile
rebar3 as prod release,tar
REPO_GATEWAY_VERSION=$(grep -o "[0-9]\+\.[0-9]\+\.[0-9]\+" apps/cvmfs_gateway/src/cvmfs_gateway.app.src)
TARBALL_NAME=cvmfs_gateway-${REPO_GATEWAY_VERSION}-${CVMFS_BUILD_PLATFORM}-x86_64.tar.gz
PKGMAP_FILE=${CVMFS_BUILD_LOCATION}/pkgmap/pkgmap.${CVMFS_BUILD_PLATFORM}_x86_64

mkdir -p ${CVMFS_BUILD_LOCATION}/tarballs
cp -v _build/prod/rel/cvmfs_gateway/cvmfs_gateway-${REPO_GATEWAY_VERSION}.tar.gz \
   ${CVMFS_BUILD_LOCATION}/tarballs/${TARBALL_NAME}
mkdir -p ${CVMFS_BUILD_LOCATION}/pkgmap
echo "[${CVMFS_BUILD_PLATFORM}_x86_64]" >> ${PKGMAP_FILE}
echo "gateway=${TARBALL_NAME}" >> ${PKGMAP_FILE}
