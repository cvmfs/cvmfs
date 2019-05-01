#!/bin/sh

set -e

SCRIPT_LOCATION=$(cd "$(dirname "$0")"; pwd)

if [ $# -lt 1 ]; then
  echo "Usage: $0 <source directory> [NIGHTLY_NUMBER]"
  echo "This script builds packages for the current platform."
  exit 1
fi

if [ -z ${CVMFS_BUILD_PLATFORM} ]; then
  echo "CVMFS_BUILD_PLATFORM unset."
  exit 1
fi

CVMFS_GATEWAY_SOURCES="$1"
shift 1

NIGHTLY_NUMBER=
if [ $# -gt 0 ]; then
  NIGHTLY_NUMBER=$1
  shift 1
fi

REPO_GATEWAY_VERSION=$(cat $CVMFS_GATEWAY_SOURCES/version)

PACKAGE_VERSION=1
if [ ! -z "$NIGHTLY_NUMBER" ]; then
    PACKAGE_VERSION=0.$NIGHTLY_NUMBER
fi

# Create an RPM or DEB package from the tarball
if [ x"${CVMFS_BUILD_PLATFORM}" = xubuntu1804 ]; then
    BUILDER_SCRIPT=${SCRIPT_LOCATION}/make_deb.sh
elif [ x"${CVMFS_BUILD_PLATFORM}" = xslc6 ] || [ x"${CVMFS_BUILD_PLATFORM}" = xcc7 ]; then
    BUILDER_SCRIPT=${SCRIPT_LOCATION}/make_rpm.sh
fi

$BUILDER_SCRIPT \
  $(cd ${CVMFS_GATEWAY_SOURCES}; pwd) \
  ${CVMFS_BUILD_PLATFORM} \
  ${REPO_GATEWAY_VERSION} \
  ${PACKAGE_VERSION}
