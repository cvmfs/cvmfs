#!/bin/bash

set -e

SCRIPT_LOCATION=$(cd "$(dirname "$0")"; pwd)
. ${SCRIPT_LOCATION}/../common.sh

SYSTEM_NAME="ubuntu1404"
BASE_ARCH="x86_64"
# REPO_BASE_URL="http://linuxsoft.cern.ch/cern/slc6X/$BASE_ARCH/yum/os/"
# GPG_KEY_PATHS="file:///etc/pki/rpm-gpg/RPM-GPG-KEY-cern"
# BASE_PACKAGES="sl-release coreutils tar iputils rpm yum yum-conf"


TARBALL_NAME="${SYSTEM_NAME}_${BASE_ARCH}.tar.gz"
DESTINATION="$(mktemp -d)"

which debootstrap || die "debootstrap is not installed"

echo "installing cleanup handler..."
cleanup() {
  echo "cleaning up the build environment..."
  umount ${DESTINATION}/dev  || true
  umount ${DESTINATION}/proc || true
  rm -fR $DESTINATION        || true
}
trap cleanup EXIT HUP INT TERM

echo "creating chroot dir..."
[ ! -d $DESTINATION ] || rm -fR $DESTINATION
mkdir -p $DESTINATION

echo "bootstrapping a build environment..."
debootstrap --variant=buildd  \
            trusty            \
            $DESTINATION      \
            http://archive.ubuntu.com/ubuntu/

echo "packaging up the image..."
tar -czf $TARBALL_NAME -C $DESTINATION .
