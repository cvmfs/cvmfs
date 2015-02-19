#!/bin/sh

#
# This script builds the default debian configuration packages for CernVM-FS.
#

set -e

SCRIPT_LOCATION=$(cd "$(dirname "$0")"; pwd)
. ${SCRIPT_LOCATION}/common.sh

if [ $# -ne 2 ]; then
  echo "Usage: $0 <CernVM-FS source directory> <build result location>"
  echo "This script builds the default CernVM-FS debian configuration package"
  exit 1
fi

CVMFS_SOURCE_LOCATION="$1"
CVMFS_RESULT_LOCATION="$2"

# sanity checks
[ ! -d ${CVMFS_SOURCE_LOCATION}/debian ]   || die "source directory seemed to be built before (${CVMFS_SOURCE_LOCATION}/debian exists)"
[ ! -f ${CVMFS_SOURCE_LOCATION}/Makefile ] || die "source directory seemed to be built before (${CVMFS_SOURCE_LOCATION}/Makefile exists)"

echo "preparing source directory for the build..."
cp -rv ${CVMFS_SOURCE_LOCATION}/packaging/debian/config-default \
       ${CVMFS_SOURCE_LOCATION}/debian
cp -v ${CVMFS_SOURCE_LOCATION}/packaging/debian/config-default/Makefile \
      ${CVMFS_SOURCE_LOCATION}/Makefile

echo "switching to the source directory..."
cd ${CVMFS_SOURCE_LOCATION}/debian

echo "running the debian package build..."
pdebuild --buildresult ${CVMFS_RESULT_LOCATION}

# clean up the source tree
echo "cleaning up..."
rm -fR ${CVMFS_SOURCE_LOCATION}/debian
rm -f  ${CVMFS_SOURCE_LOCATION}/Makefile
