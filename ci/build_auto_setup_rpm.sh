#!/bin/sh

#
# This script builds the auto setup RPM for CernVM-FS.
#

set -e

SCRIPT_LOCATION=$(cd "$(dirname "$0")"; pwd)
. ${SCRIPT_LOCATION}/common.sh

if [ $# -ne 2 ]; then
  echo "Usage: $0 <CernVM-FS source directory> <build result location>"
  echo "This script builds the CernVM-FS auto-setup RPM package"
  exit 1
fi

CVMFS_SOURCE_LOCATION="$1"
CVMFS_RESULT_LOCATION="$2"

echo "preparing the build environment in ${CVMFS_RESULT_LOCATION}..."
for d in BUILD RPMS SOURCES SRPMS TMP; do
  mkdir ${CVMFS_RESULT_LOCATION}/${d}
done

echo "copying the files to be packaged in place..."
package_spec="${CVMFS_SOURCE_LOCATION}/packaging/rpm/cvmfs-auto-setup.spec"
cp $package_spec $CVMFS_RESULT_LOCATION

echo "switching to ${CVMFS_RESULT_LOCATION}..."
cd $CVMFS_RESULT_LOCATION

# Get release version of the package to be built and build it
package_release=$(sed -e 's/^Version: \(.*\)$/\1/;tx;d;:x' $package_spec)
echo "Building release ${package_release}..."

rpmbuild --define "%_topdir $CVMFS_RESULT_LOCATION"        \
         --define "%_tmppath ${CVMFS_RESULT_LOCATION}/TMP" \
         -ba cvmfs-auto-setup.spec
