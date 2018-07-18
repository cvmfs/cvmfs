#!/bin/sh

set -e

SCRIPT_LOCATION=$(cd "$(dirname "$0")"; pwd)

if [ $# -lt 1 ]; then
  echo "Usage: $0 <CernVM-FS source directory (the same as the build directory)> [NIGHTLY_NUMBER]"
  echo "This script builds packages for the current platform."
  exit 1
fi

CVMFS_BUILD_LOCATION="$1"
shift 1

NIGHTLY_NUMBER=
if [ $# -gt 0 ]; then
  NIGHTLY_NUMBER=$1
  shift 1
fi

export REBAR_CACHE_DIR=$CVMFS_BUILD_LOCATION/../
mkdir -p $REBAR_CACHE_DIR

# run the build script
echo "switching to $CVMFS_BUILD_LOCATION..."
cd "$CVMFS_BUILD_LOCATION"
rebar3 as prod compile
rebar3 as prod release,tar
REPO_GATEWAY_VERSION=$(grep -o "[0-9]\+\.[0-9]\+\.[0-9]\+" apps/cvmfs_gateway/src/cvmfs_gateway.app.src)

PACKAGE_VERSION=1
if [ ! -z "$NIGHTLY_NUMBER" ]; then
    PACKAGE_VERSION=0.$NIGHTLY_NUMBER
fi

# Create an RPM or DEB package from the tarball
if [ x"${CVMFS_BUILD_PLATFORM}" = xubuntu1604 ] || [ x"${CVMFS_BUILD_PLATFORM}" = xubuntu1804 ]; then
    BUILDER_SCRIPT=${SCRIPT_LOCATION}/make_deb.sh
elif [ x"${CVMFS_BUILD_PLATFORM}" = xslc6 ] || [ x"${CVMFS_BUILD_PLATFORM}" = xcc7 ]; then
    BUILDER_SCRIPT=${SCRIPT_LOCATION}/make_rpm.sh
fi

$BUILDER_SCRIPT \
    _build/prod/rel/cvmfs_gateway/cvmfs_gateway-${REPO_GATEWAY_VERSION}.tar.gz \
    $(cd ${CVMFS_BUILD_LOCATION}; pwd) \
    ${CVMFS_BUILD_PLATFORM} \
    ${REPO_GATEWAY_VERSION} \
    ${PACKAGE_VERSION}
