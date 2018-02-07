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
REPO_SERVICES_VERSION=$(grep -o "[0-9]\+\.[0-9]\+\.[0-9]\+" apps/cvmfs_services/src/cvmfs_services.app.src)
TARBALL_NAME=cvmfs_services_${REPO_SERVICES_VERSION}_${CVMFS_BUILD_PLATFORM}_x86_64.tar.gz
PKGMAP_FILE=${CVMFS_BUILD_LOCATION}/pkgmap/pkgmap.${CVMFS_BUILD_PLATFORM}_x86_64

# Create an RPM or DEB package from the tarball
if [ x"${CVMFS_BUILD_PLATFORM}" = x"ubuntu1604" ]; then
    PACKAGE_TYPE=deb
    PACKAGE_NAME_SUFFIX="+${CVMFS_BUILD_PLATFORM}_amd64"
elif [ x"${CVMFS_BUILD_PLATFORM}" = xslc6 ]; then
    PACKAGE_TYPE=rpm
    PACKAGE_NAME_SUFFIX=".el6.x86_64"
elif [ x"${CVMFS_BUILD_PLATFORM}" = xcc7 ]; then
    PACKAGE_TYPE=rpm
    PACKAGE_NAME_SUFFIX=".el7.x86_64"
fi
PACKAGE_NAME=cvmfs_services_${REPO_SERVICES_VERSION}~1${PACKAGE_NAME_SUFFIX}.${PACKAGE_TYPE}

mkdir -p ${CVMFS_BUILD_LOCATION}/packages

cp -v _build/prod/rel/cvmfs_services/cvmfs_services-${REPO_SERVICES_VERSION}.tar.gz \
   ${CVMFS_BUILD_LOCATION}/packages/${TARBALL_NAME}

fpm -s tar -t ${PACKAGE_TYPE} --prefix /opt/cvmfs_services --package ${CVMFS_BUILD_LOCATION}/packages/${PACKAGE_NAME} ${CVMFS_BUILD_LOCATION}/packages/${TARBALL_NAME}

mkdir -p ${CVMFS_BUILD_LOCATION}/pkgmap
echo "[${CVMFS_BUILD_PLATFORM}_x86_64]" >> ${PKGMAP_FILE}
echo "services=${PACKAGE_NAME}" >> ${PKGMAP_FILE}
