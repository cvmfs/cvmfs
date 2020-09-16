#!/bin/sh

#
# This script builds the CernVM-FS service container in OCI and docker archive.
# The cvmfs-service-$version.x86_64.docker.tar.gz archive can be added to docker
# with `cat $archive | docker load`
#

set -e

SCRIPT_LOCATION=$(cd "$(dirname "$0")"; pwd)

CVMFS_RESULT_LOCATION="$1"
CVMFS_BUSYBOX_URL=${2+"https://ecsft.cern.ch/dist/cvmfs/builddeps/busybox"}
CVMFS_TAG="$3"

[ ! -z $CVMFS_RESULT_LOCATION ] || exit 1
[ ! -z $CVMFS_BUSYBOX_URL ] || exit 1
[ ! -z $CVMFS_TAG ] || exit 1

IMAGE_NAME="cvmfs/service:$CVMFS_TAG"
ARCHIVE_NAME="cvmfs-service-$CVMFS_TAG"

echo $SCRIPT_LOCATION

docker build \
        --tag ${IMAGE_NAME} \
        --build-arg CVMFS_COMMIT=$CVMFS_TAG \
        --build-arg BUSYBOX_URL=$CVMFS_BUSYBOX_URL \
        $SCRIPT_LOCATION

docker save \
  --output ${CVMFS_RESULT_LOCATION}/${ARCHIVE_NAME}.docker.tar \
  $IMAGE_NAME
docker rmi $IMAGE_NAME

gzip --force ${CVMFS_RESULT_LOCATION}/${ARCHIVE_NAME}.docker.tar
