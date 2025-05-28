#!/bin/sh

#
# This script builds the default debian configuration packages for CernVM-FS.
#

set -e

SCRIPT_LOCATION=$(cd "$(dirname "$0")"; pwd)
. ${SCRIPT_LOCATION}/../common.sh

if [ $# -ne 2 ]; then
  echo "Usage: $0 <CernVM-FS source directory> <build result location>"
  echo "This script builds the default CernVM-FS debian configuration package"
  exit 1
fi

CVMFS_SOURCE_LOCATION="$1"
CVMFS_RESULT_LOCATION="$2"

# sanity checks
[ ! -d ${CVMFS_RESULT_LOCATION}/cvmfs-config/debian ]   || die "source directory seemed to be built before (${CVMFS_RESULT_LOCATION}/cvmfs-config/debian exists)"
[ ! -f ${CVMFS_RESULT_LOCATION}/cvmfs-config/Makefile ] || die "source directory seemed to be built before (${CVMFS_RESULT_LOCATION}/cvmfs-config/Makefile exists)"

# build wrapper for cvmfs-config-* package
build_config_package() {
  local config_package="$1"

  mkdir -p ${CVMFS_RESULT_LOCATION}/cvmfs-config
  echo "preparing source directory for the build ($config_package)..."
  cp -rv ${CVMFS_SOURCE_LOCATION}/packaging/debian/${config_package} \
         ${CVMFS_RESULT_LOCATION}/cvmfs-config/debian
  cp -v ${CVMFS_SOURCE_LOCATION}/packaging/debian/${config_package}/Makefile \
        ${CVMFS_RESULT_LOCATION}/cvmfs-config/Makefile

  cp -rv ${CVMFS_SOURCE_LOCATION}/mount \
         ${CVMFS_RESULT_LOCATION}/cvmfs-config/

  echo "switching to the debian source directory..."
  cd ${CVMFS_RESULT_LOCATION}/cvmfs-config/debian

  echo "running the debian package build ($config_package)..."
  debuild  --no-tgz-check --check-dirname-level 0 -us -uc # -us -uc == skip signing
  #mv ${CVMFS_SOURCE_LOCATION}/../cvmfs-config-*_* ${CVMFS_RESULT_LOCATION}/

  #echo "switching back to the source directory..."
  #cd ${CVMFS_SOURCE_LOCATION}

  echo "cleaning up..."
  rm -fR ${CVMFS_RESULT_LOCATION}/cvmfs-config
}

echo "build the config packages..."
build_config_package "config-default"
#build_config_package "config-none"
#build_config_package "config-graphdriver"
#build_config_package "config-shrinkwrap"
