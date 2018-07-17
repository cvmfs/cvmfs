#!/bin/bash

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
TARBALL_NAME=cvmfs-gateway_${REPO_GATEWAY_VERSION}_${CVMFS_BUILD_PLATFORM}_x86_64.tar.gz
PKGMAP_FILE=${CVMFS_BUILD_LOCATION}/pkgmap/pkgmap.${CVMFS_BUILD_PLATFORM}_x86_64

PACKAGE_VERSION=1
if [ ! -z "$NIGHTLY_NUMBER" ]; then
    PACKAGE_VERSION=0.$NIGHTLY_NUMBER
fi

# Create an RPM or DEB package from the tarball
if [ x"${CVMFS_BUILD_PLATFORM}" = xubuntu1604 ]; then
    PACKAGE_TYPE=deb
    PACKAGE_NAME_SUFFIX="+ubuntu16.04_amd64"
    PACKAGE_LOCATION=DEBS
    PACKAGE_NAME=cvmfs-gateway_${REPO_GATEWAY_VERSION}~${PACKAGE_VERSION}${PACKAGE_NAME_SUFFIX}.${PACKAGE_TYPE}

    mkdir -p ${CVMFS_BUILD_LOCATION}/$PACKAGE_LOCATION

    cp -v _build/prod/rel/cvmfs_gateway/cvmfs_gateway-${REPO_GATEWAY_VERSION}.tar.gz \
    ${CVMFS_BUILD_LOCATION}/$PACKAGE_LOCATION/${TARBALL_NAME}

    # Create the distribution-specific package

    if [ -e /etc/profile.d/rvm.sh ]; then
        . /etc/profile.d/rvm.sh
    fi

    fpm -s tar \
        -t ${PACKAGE_TYPE} \
        --prefix /usr/libexec/cvmfs-gateway \
        --package $PACKAGE_LOCATION/${PACKAGE_NAME} \
        --version ${REPO_GATEWAY_VERSION} \
        --name cvmfs-gateway \
        --maintainer "Radu Popescu <radu.popescu@cern.ch>" \
        --description "CernVM-FS Repository Gateway" \
        --url "http://cernvm.cern.ch" \
        --license "BSD-3-Clause" \
        $PACKAGE_LOCATION/${TARBALL_NAME}

    mkdir -p ${CVMFS_BUILD_LOCATION}/pkgmap
    echo "[${CVMFS_BUILD_PLATFORM}_x86_64]" >> ${PKGMAP_FILE}
    echo "gateway=${PACKAGE_NAME}" >> ${PKGMAP_FILE}

elif [ x"${CVMFS_BUILD_PLATFORM}" = xslc6 ] || [ x"${CVMFS_BUILD_PLATFORM}" = xcc7 ]; then
    ${SCRIPT_LOCATION}/make_rpm.sh \
        _build/prod/rel/cvmfs_gateway/cvmfs_gateway-${REPO_GATEWAY_VERSION}.tar.gz \
        $(cd ${CVMFS_BUILD_LOCATION}; pwd) \
        ${CVMFS_BUILD_PLATFORM} \
        ${REPO_GATEWAY_VERSION} \
        ${PACKAGE_VERSION}
fi

