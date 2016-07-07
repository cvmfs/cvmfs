#!/bin/sh

#
# This script creates the source tarball of CernVM-FS.
#

set -e

SCRIPT_LOCATION=$(cd "$(dirname "$0")"; pwd)
. ${SCRIPT_LOCATION}/common.sh

if [ $# -lt 2 ]; then
  echo "Usage: $0 <CernVM-FS source directory> <build result location> [<nightly build number>]"
  echo "This script creates the CernVM-FS source tarball"
  exit 1
fi

CVMFS_SOURCE_LOCATION="$1"
CVMFS_RESULT_LOCATION="$2"
CVMFS_NIGHTLY_BUILD_NUMBER="${3-0}"

# retrieve the upstream version string from CVMFS
cvmfs_version="$(get_cvmfs_version_from_cmake $CVMFS_SOURCE_LOCATION)"
git_hash="$(get_cvmfs_git_revision $CVMFS_SOURCE_LOCATION)"
echo "detected upstream version: $cvmfs_version"
echo "detected treeish ${git_hash}"

if [ $CVMFS_NIGHTLY_BUILD_NUMBER -gt 0 ]; then
  tarball="cvmfs-git-${git_hash}.tar.gz"
  echo "Using nightly version scheme: $tarball"
else
  tarball="cvmfs-${cvmfs_version}.tar.gz"
  echo "Creating release tarball $tarball"
fi

echo "create a source tarball build location..."
CVMFS_TARBALL_RESULT_LOCATION="${CVMFS_RESULT_LOCATION}/cvmfs_source_tarball"
[ ! -d ${CVMFS_TARBALL_RESULT_LOCATION} ] || rm -fR ${CVMFS_TARBALL_RESULT_LOCATION}
mkdir -p $CVMFS_TARBALL_RESULT_LOCATION

echo "creating source tar ball '$tarball'..."
create_cvmfs_source_tarball ${CVMFS_SOURCE_LOCATION} \
                            ${CVMFS_TARBALL_RESULT_LOCATION}/${tarball}
cp ${CVMFS_TARBALL_RESULT_LOCATION}/${tarball} \
   ${CVMFS_TARBALL_RESULT_LOCATION}/source.tar.gz
