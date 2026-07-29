#!/bin/sh

#
# Builds the CernVM-FS service container in docker archive form. Compilation
# and dependency resolution happen inside the multi-stage Dockerfile, so the
# host only needs docker + buildkit.
#
# The cvmfs-service-$version.<arch>.docker.tar.gz archive can be loaded with
#   docker load < $archive
#

set -e

SCRIPT_LOCATION=$(cd "$(dirname "$0")"; pwd)

CVMFS_SOURCE_LOCATION="$1"
CVMFS_RESULT_LOCATION="$2"
# The 3rd positional argument used to be the host busybox; busybox is now
# installed in the builder stage. For backward compatibility with older
# callers we accept either signature.
if [ $# -ge 4 ]; then
  CVMFS_TAG="$4"
else
  CVMFS_TAG="$3"
fi
BASE_IMAGE="${BASE_IMAGE:-almalinux:9}"

[ -n "$CVMFS_SOURCE_LOCATION" ] || exit 1
[ -n "$CVMFS_RESULT_LOCATION" ] || exit 1
[ -n "$CVMFS_TAG" ]             || exit 1

mkdir -p "$CVMFS_RESULT_LOCATION"

IMAGE_NAME="cvmfs/service:$CVMFS_TAG"
ARCHIVE_NAME="cvmfs-service-${CVMFS_TAG}.$(uname -m)"

DOCKER_BUILDKIT=1 docker build \
  --build-arg "VERSION=$CVMFS_TAG" \
  --build-arg "PLATFORM=$BASE_IMAGE" \
  --build-arg "BASE_IMAGE=$BASE_IMAGE" \
  --file "$CVMFS_SOURCE_LOCATION/packaging/container/Dockerfile" \
  --tag "$IMAGE_NAME" \
  "$CVMFS_SOURCE_LOCATION"

docker inspect "$IMAGE_NAME"
docker save --output "$CVMFS_RESULT_LOCATION/$ARCHIVE_NAME.docker.tar" "$IMAGE_NAME"
docker rmi "$IMAGE_NAME"

gzip --force "$CVMFS_RESULT_LOCATION/$ARCHIVE_NAME.docker.tar"
