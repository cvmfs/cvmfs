#!/bin/sh

#
# This script builds the CernVM-FS service container.
#

set -e

SCRIPT_LOCATION=$(cd "$(dirname "$0")"; pwd)
. ${SCRIPT_LOCATION}/../common.sh

if [ $# -lt 2 ]; then
  echo "Usage: $0 <CernVM-FS source directory> <build result location> [<nightly build number>]"
  echo "This script builds CernVM-FS service container"
  exit 1
fi

CVMFS_SOURCE_LOCATION="$1" # not used
CVMFS_RESULT_LOCATION="$2"
CVMFS_BUSYBOX_URL="https://ecsft.cern.ch/dist/cvmfs/builddeps/busybox"
CVMFS_NIGHTLY_BUILD_NUMBER="${3-0}"

# For the time being, build with the host's docker until the builder nodes are
# new enough to support fuse in user namespaces.  That's a precondition to
# using buildah

if ! docker version; then
  echo "docker required to build container image"
  exit 1
fi

# retrieve the upstream version string from CVMFS
cvmfs_version="$(get_cvmfs_version_from_cmake $CVMFS_SOURCE_LOCATION)"
echo "detected upstream version: $cvmfs_version"

git_hash="$(get_cvmfs_git_revision $CVMFS_SOURCE_LOCATION)"

# generate the release tag for either a nightly build or a release
CVMFS_TAG=
if [ $CVMFS_NIGHTLY_BUILD_NUMBER -gt 0 ]; then
  build_tag="git-${git_hash}"
  nightly_tag="0.${CVMFS_NIGHTLY_BUILD_NUMBER}.${git_hash}git"

  echo "creating nightly build '$nightly_tag'"
  CVMFS_TAG="${cvmfs_version}-$nightly_tag"
else
  echo "creating release: $cvmfs_version"
  CVMFS_TAG="${cvmfs_version}-1"
fi

${CVMFS_SOURCE_LOCATION}/packaging/container/build.sh \
  ${CVMFS_RESULT_LOCATION} ${CVMFS_BUSYBOX_URL} ${git_hash} \
  || die "failed building service container"


# generating package map section for specific platform
if [ ! -z $CVMFS_CI_PLATFORM_LABEL ]; then
  echo "generating package map section for ${CVMFS_CI_PLATFORM_LABEL}..."
  generate_package_map                                      \
    "$CVMFS_CI_PLATFORM_LABEL"                              \
    "cvmfs-service-${CVMFS_TAG}.$(uname -m).docker.tar.gz"  \
    ""  \
    ""  \
    ""  \
    ""  \
    ""
fi
