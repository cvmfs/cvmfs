#!/bin/sh

#
# This script builds the debian packages of CernVM-FS.
#

set -e

SCRIPT_LOCATION=$(cd "$(dirname "$0")"; pwd)
. ${SCRIPT_LOCATION}/common.sh

if [ $# -lt 2 ]; then
  echo "Usage: $0 <CernVM-FS source directory> <build result location> [<nightly build number>]"
  echo "This script builds CernVM-FS debian packages"
  exit 1
fi

CVMFS_SOURCE_LOCATION="$1"
CVMFS_RESULT_LOCATION="$2"
CVMFS_NIGHTLY_BUILD_NUMBER="${3-0}"

CVMFS_CONFIG_PACKAGE="cvmfs-config-default_1.1-1_all.deb"

# sanity checks
[ ! -d ${CVMFS_SOURCE_LOCATION}/debian ] || die "source directory seemed to be built before (${CVMFS_SOURCE_LOCATION}/debian exists)"

# retrieve the upstream version string from CVMFS
cvmfs_version="$(get_cvmfs_version_from_cmake $CVMFS_SOURCE_LOCATION)"
echo "detected upstream version: $cvmfs_version"

# generate the release tag for either a nightly build or a release
if [ $CVMFS_NIGHTLY_BUILD_NUMBER -gt 0 ]; then
  git_hash="$(get_cvmfs_git_revision $CVMFS_SOURCE_LOCATION)"
  cvmfs_version="${cvmfs_version}.${CVMFS_NIGHTLY_BUILD_NUMBER}git${git_hash}"
  echo "creating nightly build '$cvmfs_version'"
else
  echo "creating release: $cvmfs_version"
fi

# produce the debian package
echo "copy packaging meta information and get in place..."
cp -r ${CVMFS_SOURCE_LOCATION}/packaging/debian/cvmfs ${CVMFS_SOURCE_LOCATION}/debian
mkdir -p $CVMFS_RESULT_LOCATION
cd ${CVMFS_SOURCE_LOCATION}

echo "do the build..."
dch -v $cvmfs_version -M "bumped upstream version number"
cd debian
pdebuild --buildresult $CVMFS_RESULT_LOCATION
cd ${CVMFS_RESULT_LOCATION}

# generating package map section for specific platform
if [ ! -z $CVMFS_CI_PLATFORM_LABEL ]; then
  echo "generating package map section for ${CVMFS_CI_PLATFORM_LABEL}..."
  generate_package_map "$CVMFS_CI_PLATFORM_LABEL"                           \
                       "$(basename $(find . -name 'cvmfs_*.deb'))"          \
                       "$(basename $(find . -name 'cvmfs-server*.deb'))"    \
                       "$(basename $(find . -name 'cvmfs-unittests*.deb'))" \
                       "$CVMFS_CONFIG_PACKAGE"
fi

# clean up the source tree
echo "cleaning up..."
rm -fR ${CVMFS_SOURCE_LOCATION}/debian
