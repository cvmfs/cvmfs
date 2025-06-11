#!/bin/sh

#
# This script builds the cvmfs-release package for debian.
#

set -e

SCRIPT_LOCATION=$(cd "$(dirname "$0")"; pwd)
. ${SCRIPT_LOCATION}/../common.sh

if [ $# -ne 2 ]; then
  echo "Usage: $0 <CernVM-FS source directory> <build result location>"
  echo "This script builds the default CernVM-FS debian configuration package"
  exit 1
fi

CVMFS_SOURCE_LOCATION="$1"
CVMFS_RESULT_LOCATION="$2"
CVMFS_BUILD_LOCATION="$2/build/cvmfs-release"
mkdir -p $CVMFS_BUILD_LOCATION


# sanity checks
[ ! -d ${CVMFS_BUILD_LOCATION}/debian ]   || die "source directory seemed to be built before (${CVMFS_BUILD_LOCATION}/debian exists)"
[ ! -f ${CVMFS_BUILD_LOCATION}/Makefile ] || die "source directory seemed to be built before (${CVMFS_BUILD_LOCATION}/Makefile exists)"

echo "preparing source directory for the build ($config_package)..."

#TODO: adapt Makefile, copy only .list files
cp -r $CVMFS_SOURCE_LOCATION/packaging $CVMFS_BUILD_LOCATION 

cp -rv ${CVMFS_SOURCE_LOCATION}/packaging/debian/cvmfs-release \
       ${CVMFS_BUILD_LOCATION}/debian
cp -v ${CVMFS_SOURCE_LOCATION}/packaging/debian/cvmfs-release/Makefile \
      ${CVMFS_BUILD_LOCATION}/Makefile

echo "switching to the debian source directory..."
cd ${CVMFS_BUILD_LOCATION}/debian

echo "running the debian package build..."
debuild --no-tgz-check -us -uc # -us -uc == skip signing
mv ${CVMFS_BUILD_LOCATION}/../cvmfs-release_* ${CVMFS_RESULT_LOCATION}/


echo "cleaning up..."
rm -fR ${CVMFS_BUILD_LOCATION}/debian
rm -f  ${CVMFS_BUILD_LOCATION}/Makefile

echo "switching back to the source directory..."
cd ${CVMFS_SOURCE_LOCATION}
