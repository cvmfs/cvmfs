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

CVMFS_SOURCE_LOCATION="$1"
CVMFS_RESULT_LOCATION="$2"
CVMFS_NIGHTLY_BUILD_NUMBER="${3-0}"

# The build itself runs inside the multi-stage Dockerfile, so all the host
# needs is a working docker with BuildKit support.
if ! docker version; then
  echo "docker required to build container image"
  exit 1
fi

# Preserve the legacy nightly-number interface.
set_cvmfs_version_sequence_from_nightly "$CVMFS_NIGHTLY_BUILD_NUMBER"

cvmfs_version="$(get_cvmfs_version "$CVMFS_SOURCE_LOCATION")"
# OCI tags do not allow '~' or '+'.
cvmfs_tag_version=$(echo "$cvmfs_version" | tr '~+' '--')
echo "detected upstream version: $cvmfs_version"

CVMFS_TAG="${cvmfs_tag_version}-1"
echo "creating container: $CVMFS_TAG"

${CVMFS_SOURCE_LOCATION}/packaging/container/build.sh \
  ${CVMFS_SOURCE_LOCATION} ${CVMFS_RESULT_LOCATION} ${CVMFS_TAG} \
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
