#!/bin/sh

#
# This script builds the default RPM configuration packages for CernVM-FS.
#

set -e

SCRIPT_LOCATION=$(cd "$(dirname "$0")"; pwd)
. ${SCRIPT_LOCATION}/../common.sh

if [ $# -ne 2 ]; then
  echo "Usage: $0 <CernVM-FS source directory> <build result location>"
  echo "This script builds the default CernVM-FS RPM configuration package"
  exit 1
fi

CVMFS_SOURCE_LOCATION="$1"
CVMFS_RESULT_LOCATION="$2"

echo "preparing the build environment in ${CVMFS_RESULT_LOCATION}..."
for d in BUILD RPMS SOURCES SRPMS TMP; do
  mkdir ${CVMFS_RESULT_LOCATION}/${d}
done

echo "copying the files to be packaged in place..."
cp ${CVMFS_SOURCE_LOCATION}/packaging/rpm/cvmfs-config-default.spec ${CVMFS_RESULT_LOCATION}
cp ${CVMFS_SOURCE_LOCATION}/packaging/rpm/cvmfs-config-none.spec    ${CVMFS_RESULT_LOCATION}

cp ${CVMFS_SOURCE_LOCATION}/mount/keys/cern.ch.pub                  ${CVMFS_RESULT_LOCATION}/SOURCES
cp ${CVMFS_SOURCE_LOCATION}/mount/keys/cern-it1.cern.ch.pub         ${CVMFS_RESULT_LOCATION}/SOURCES
cp ${CVMFS_SOURCE_LOCATION}/mount/keys/cern-it2.cern.ch.pub         ${CVMFS_RESULT_LOCATION}/SOURCES
cp ${CVMFS_SOURCE_LOCATION}/mount/keys/cern-it3.cern.ch.pub         ${CVMFS_RESULT_LOCATION}/SOURCES
cp ${CVMFS_SOURCE_LOCATION}/mount/keys/egi.eu.pub                   ${CVMFS_RESULT_LOCATION}/SOURCES
cp ${CVMFS_SOURCE_LOCATION}/mount/keys/opensciencegrid.org.pub      ${CVMFS_RESULT_LOCATION}/SOURCES
cp ${CVMFS_SOURCE_LOCATION}/mount/domain.d/cern.ch.conf             ${CVMFS_RESULT_LOCATION}/SOURCES
cp ${CVMFS_SOURCE_LOCATION}/mount/domain.d/egi.eu.conf              ${CVMFS_RESULT_LOCATION}/SOURCES
cp ${CVMFS_SOURCE_LOCATION}/mount/domain.d/opensciencegrid.org.conf ${CVMFS_RESULT_LOCATION}/SOURCES
cp ${CVMFS_SOURCE_LOCATION}/mount/default.d/50-cern.conf            ${CVMFS_RESULT_LOCATION}/SOURCES
cp ${CVMFS_SOURCE_LOCATION}/mount/default.d/60-egi.conf             ${CVMFS_RESULT_LOCATION}/SOURCES
cp ${CVMFS_SOURCE_LOCATION}/mount/config.d/*.cern.ch.conf           ${CVMFS_RESULT_LOCATION}/SOURCES

echo "switching into the build directory..."
cd ${CVMFS_RESULT_LOCATION}

echo "building RPM packages..."
rpmbuild --define "%_topdir $(pwd)"      \
         --define "%_tmppath $(pwd)/TMP" \
         -ba cvmfs-config-default.spec

rpmbuild --define "%_topdir $(pwd)"      \
         --define "%_tmppath $(pwd)/TMP" \
         -ba cvmfs-config-none.spec
