#!/bin/sh

#
# This script builds the cvmfs-release RPM for CernVM-FS.
#

set -e

SCRIPT_LOCATION=$(cd "$(dirname "$0")"; pwd)
. ${SCRIPT_LOCATION}/../common.sh

if [ $# -ne 2 ]; then
  echo "Usage: $0 <CernVM-FS source directory> <build result location>"
  echo "This script builds the CernVM-FS release RPM package"
  exit 1
fi

CVMFS_SOURCE_LOCATION="$1"
CVMFS_RESULT_LOCATION="$2"

echo "preparing the build environment in ${CVMFS_RESULT_LOCATION}..."
for d in BUILD RPMS SOURCES SRPMS TMP; do
  mkdir ${CVMFS_RESULT_LOCATION}/${d}
done

echo "copying the files to be packaged in place..."
package_spec="${CVMFS_SOURCE_LOCATION}/packaging/rpm/cvmfs-release.spec"
cp ${CVMFS_SOURCE_LOCATION}/packaging/rpm/cvmfs-release.spec  ${CVMFS_RESULT_LOCATION}
cp ${CVMFS_SOURCE_LOCATION}/packaging/rpm/BSD                 ${CVMFS_RESULT_LOCATION}/SOURCES/
cp ${CVMFS_SOURCE_LOCATION}/packaging/rpm/RPM-GPG-KEY-CernVM  ${CVMFS_RESULT_LOCATION}/SOURCES/
cp ${CVMFS_SOURCE_LOCATION}/packaging/rpm/cernvm.repo         ${CVMFS_RESULT_LOCATION}/SOURCES/
cp $package_spec                                              ${CVMFS_RESULT_LOCATION}

echo "switching to ${CVMFS_RESULT_LOCATION}..."
cd $CVMFS_RESULT_LOCATION

# Make RPM
echo "Building cvmfs-release..."
rpmbuild --define "%_topdir $CVMFS_RESULT_LOCATION"        \
         --define "%_tmppath ${CVMFS_RESULT_LOCATION}/TMP" \
         -ba cvmfs-release.spec
