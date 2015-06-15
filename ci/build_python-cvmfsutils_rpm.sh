#!/bin/sh

#
# This script builds the python-cvmfsutils RPM for CernVM-FS.
#

set -e

SCRIPT_LOCATION=$(cd "$(dirname "$0")"; pwd)
. ${SCRIPT_LOCATION}/common.sh

if [ $# -ne 2 ]; then
  echo "Usage: $0 <CernVM-FS source directory> <build result location>"
  echo "builds an RPM from the CernVM-FS python package sources"
  exit 1
fi

CVMFS_SOURCE_LOCATION="$1"
CVMFS_RESULT_LOCATION="$2"

PYTHON_BUILD_SRC=${CVMFS_RESULT_LOCATION}/py

echo "preparing the build environment in ${CVMFS_RESULT_LOCATION}..."
mkdir -p $PYTHON_BUILD_SRC             \
         ${CVMFS_RESULT_LOCATION}/RPMS \
         ${CVMFS_RESULT_LOCATION}/SRPMS

echo "copying the files to be packaged in place..."
cp -R ${CVMFS_SOURCE_LOCATION}/python/cvmfs $PYTHON_BUILD_SRC
cp ${CVMFS_SOURCE_LOCATION}/python/ChangeLog   \
   ${CVMFS_SOURCE_LOCATION}/python/COPYING     \
   ${CVMFS_SOURCE_LOCATION}/python/MANIFEST.in \
   ${CVMFS_SOURCE_LOCATION}/python/README      \
   ${CVMFS_SOURCE_LOCATION}/python/setup.py    \
   $PYTHON_BUILD_SRC

echo "switching to ${PYTHON_BUILD_SRC}..."
cd $PYTHON_BUILD_SRC

echo "build the RPM package..."
python setup.py bdist_rpm                                                             \
  --requires 'python-requests >= 1.1.0, python-dateutil >= 1.4.1, m2crypto >= 0.20.0' \
  --dist-dir "$CVMFS_RESULT_LOCATION"                                                 \
  --build-requires 'python-setuptools >= 0.6.10'                                      \
  --release "1%{?dist}"  # appends RPM dist tag (el6, el7, fc19, ...)

echo "switching to ${CVMFS_RESULT_LOCATION}"
cd ${CVMFS_RESULT_LOCATION}

echo "move the RPMs into the conventional RPM directories"
mv *.src.rpm SRPMS
mv *.rpm     RPMS
