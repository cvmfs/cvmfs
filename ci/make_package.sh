#!/bin/bash

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
TARBALL_NAME=cvmfs_gateway_${REPO_GATEWAY_VERSION}_${CVMFS_BUILD_PLATFORM}_x86_64.tar.gz
PKGMAP_FILE=${CVMFS_BUILD_LOCATION}/pkgmap/pkgmap.${CVMFS_BUILD_PLATFORM}_x86_64

# Create an RPM or DEB package from the tarball
if [ x"${CVMFS_BUILD_PLATFORM}" = xubuntu1604 ]; then
    PACKAGE_TYPE=deb
    PACKAGE_NAME_SUFFIX="+ubuntu16.04_amd64"
elif [ x"${CVMFS_BUILD_PLATFORM}" = xslc6 ] || [ x"${CVMFS_BUILD_PLATFORM}" = xcc7 ]; then
    PACKAGE_TYPE=rpm
    PACKAGE_NAME_SUFFIX="$(rpm --eval "%{?dist}").x86_64"
fi
PACKAGE_NAME=cvmfs_gateway_${REPO_GATEWAY_VERSION}~1${PACKAGE_NAME_SUFFIX}.${PACKAGE_TYPE}

mkdir -p ${CVMFS_BUILD_LOCATION}/packages

cp -v _build/prod/rel/cvmfs_gateway/cvmfs_gateway-${REPO_GATEWAY_VERSION}.tar.gz \
   ${CVMFS_BUILD_LOCATION}/packages/${TARBALL_NAME}

# Create the distribution-specific package

if [ -e /etc/profile.d/rvm.sh ]; then
    . /etc/profile.d/rvm.sh
fi

fpm -s tar \
    -t ${PACKAGE_TYPE} \
    --prefix /opt/cvmfs_gateway \
    --package packages/${PACKAGE_NAME} \
    --version ${REPO_GATEWAY_VERSION} \
    --name cvmfs_gateway \
    --maintainer "Radu Popescu <radu.popescu@cern.ch>" \
    --description "CernVM-FS Repository Gateway" \
    --url "http://cernvm.cern.ch" \
    --license "BSD-3-Clause" \
    packages/${TARBALL_NAME}

mkdir -p ${CVMFS_BUILD_LOCATION}/pkgmap
echo "[${CVMFS_BUILD_PLATFORM}_x86_64]" >> ${PKGMAP_FILE}
echo "gateway=${PACKAGE_NAME}" >> ${PKGMAP_FILE}

