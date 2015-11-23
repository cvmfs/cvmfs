#!/bin/bash

set -e

SCRIPT_LOCATION=$(cd "$(dirname "$0")"; pwd)
. ${SCRIPT_LOCATION}/../common.sh

SYSTEM_NAME="sles11"
BASE_ARCH="x86_64"

TARBALL_NAME="${SYSTEM_NAME}_${BASE_ARCH}.tar.gz"
DESTINATION="$(mktemp -d)"

echo "installing cleanup handler..."
cleanup() {
  echo "cleaning up the build environment..."
  rm -fR $DESTINATION || true
}
trap cleanup EXIT HUP INT TERM

echo "creating chroot dir..."
[ ! -d $DESTINATION ] || rm -fR $DESTINATION
mkdir -p $DESTINATION

echo "running essential build script in a SuSE container..."
docker run --volume ${DESTINATION}:/chroot    \
           --volume ${SCRIPT_LOCATION}:/build \
           --rm=true                          \
           --privileged=true                  \
           opensuse:latest /build/build_internal.sh

echo "package up the base image..."
tar -czf $TARBALL_NAME -C $DESTINATION .
