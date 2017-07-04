#!/bin/sh

set -e

#
# This script builds the stretch autofs package on older distros.
#

SCRIPT_LOCATION=$(cd "$(dirname "$0")"; pwd)
. ${SCRIPT_LOCATION}/../common.sh

usage() {
  echo "Build the Debian stretch autofs version for older deb based distros"
  echo "Usage: $0 <work dir> <result dir>"
  exit 1
}

if [ $# -ne 2 ]; then
  usage
  exit 1
fi

CVMFS_SOURCE_LOCATION="$1"
CVMFS_RESULT_LOCATION="$2"

workdir="$CVMFS_SOURCE_LOCATION/_DEBAUTOFS"
mkdir -p $workdir

if [ "$(ls -A $workdir 2>/dev/null)" != "" ]; then
  echo "$workdir must be empty"
  exit 2
fi

echo -n "creating workspace in $workdir... "
mkdir ${workdir}/src ${workdir}/result
echo "done"

echo -n "getting autofs source package"
cd ${workdir}/src
codename=$(lsb_release -sc)
if [ "x$codename" = "xjessie" ]; then
  apt-get source autofs/stretch
elif [ "x$codename" = "xxenial" ]; then
  apt-get source autofs/zesty
else
  echo "Distribution $codename not supported!"
  exit 1
fi
srcdir=$(find . -mindepth 1 -maxdepth 1 -type d)
cd $srcdir
dpkg-checkbuilddeps
echo "done"

echo -n "adjust package version"
version=$(head -n 1 debian/changelog | cut -d" " -f2 | tr -d \(\))
platform="$(lsb_release -si | tr [:upper:] [:lower:])$(lsb_release -sr)"
version="${version}cernvm1+${platform}"
export DEBFULLNAME="Jakob Blomer"
export DEBEMAIL="jblomer@cern.ch"
dch -v $version "rebuild stretch autofs for recursive mounting support"
dch -r --distribution $(lsb_release -sc) ""
echo "done"

debuild -us -uc
mv ${workdir}/src/autofs_*cernvm* ${CVMFS_RESULT_LOCATION}/

echo "cleaning up..."
rm -fR ${CVMFS_SOURCE_LOCATION}/*
