#!/bin/sh

set -e

#
# This script builds the stretch autofs package on older distros.
#

SCRIPT_LOCATION=$(cd "$(dirname "$0")"; pwd)
. ${SCRIPT_LOCATION}/../common.sh

usage() {
  echo "Build the Debian stretch autofs version for older deb based distros"
  echo "Usage: $0 <work dir>"
  exit 1
}

if [ $# -ne 2 ]; then
  usage
  exit 1
fi

CVMFS_SOURCE_LOCATION="$1"
CVMFS_RESULT_LOCATION="$2"

workdir="$CVMFS_SOURCE_LOCATION"

if [ "$(ls -A $workdir 2>/dev/null)" != "" ]; then
  echo "$workdir must be empty"
  exit 2
fi

echo -n "creating workspace in $workdir... "
mkdir ${workdir}/src ${workdir}/result
echo "done"

echo -n "getting autofs source package"
cd ${workdir}/src
apt-get source autofs/stretch
srcdir=$(find . -mindepth 1 -maxdepth 1 -type d)
cd $srcdir
dpkg-checkbuilddeps
dch --local cernvm "rebuild stretch autofs for recursive mounting support"
echo "done"

debuild -us -uc
mv ${workdir}/src/autofs_*cernvm* ${CVMFS_RESULT_LOCATION}/

echo "cleaning up..."
rm -fR ${CVMFS_SOURCE_LOCATION}/*
