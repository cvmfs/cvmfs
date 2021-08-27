#!/bin/sh

#
# This script builds the default APS configuration packages for CernVM-FS.
#

set -e

SCRIPT_LOCATION=$(cd "$(dirname "$0")"; pwd)
. ${SCRIPT_LOCATION}/../common.sh

if [ $# -ne 2 ]; then
  echo "Usage: $0 <CernVM-FS source directory> <build result location>"
  echo "This script builds the default CernVM-FS APS configuration package"
  exit 1
fi

CVMFS_SOURCE_LOCATION="$1"
CVMFS_RESULT_LOCATION="$2"

PKGROOT_DEFAULT="$CVMFS_SOURCE_LOCATION/packaging/aps/cvmfs-config-default"
PKGROOT_NONE="$CVMFS_SOURCE_LOCATION/packaging/aps/cvmfs-config-none"

echo "copying the files to be packaged in place..."
cp ${CVMFS_SOURCE_LOCATION}/mount/keys/cern-it1.cern.ch.pub $PKGROOT_DEFAULT
cp ${CVMFS_SOURCE_LOCATION}/mount/keys/cern-it2.cern.ch.pub $PKGROOT_DEFAULT
cp ${CVMFS_SOURCE_LOCATION}/mount/keys/cern-it3.cern.ch.pub $PKGROOT_DEFAULT
cp ${CVMFS_SOURCE_LOCATION}/mount/keys/egi.eu.pub $PKGROOT_DEFAULT
cp ${CVMFS_SOURCE_LOCATION}/mount/keys/opensciencegrid.org.pub $PKGROOT_DEFAULT
cp ${CVMFS_SOURCE_LOCATION}/mount/domain.d/cern.ch.conf $PKGROOT_DEFAULT
cp ${CVMFS_SOURCE_LOCATION}/mount/domain.d/egi.eu.conf $PKGROOT_DEFAULT
cp ${CVMFS_SOURCE_LOCATION}/mount/domain.d/opensciencegrid.org.conf \
  $PKGROOT_DEFAULT
cp ${CVMFS_SOURCE_LOCATION}/mount/default.d/50-cern.conf $PKGROOT_DEFAULT
cp ${CVMFS_SOURCE_LOCATION}/mount/config.d/*.cern.ch.conf $PKGROOT_DEFAULT

echo "switching into the package root directory for config-default..."
cd $PKGROOT_DEFAULT
makepkg -g >> PKGBUILD
makepkg

echo "moving config-default packages to result location..."
mv *.pkg.tar.xz ${CVMFS_RESULT_LOCATION}/

echo "switching into the package root directory for config-none..."
cd $OLDPWD
cd $PKGROOT_NONE
makepkg

echo "moving config-none packages to result location..."
mv *.pkg.tar.xz ${CVMFS_RESULT_LOCATION}/
