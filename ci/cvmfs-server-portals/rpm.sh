#!/bin/sh

#
# This script builds the RPM packages of CernVM-FS server portals add-ons.
#

set -e

SCRIPT_LOCATION=$(cd "$(dirname "$0")"; pwd)
. ${SCRIPT_LOCATION}/../common.sh

if [ $# -lt 2 ]; then
  echo "Usage: $0 <directory with checked out sources> <build result location>"
  echo "This script builds CernVM-FS server portals add-on package"
  exit 1
fi

CVMFS_SOURCE_LOCATION="$1"
ALL_SOURCE_LOCATION="${CVMFS_SOURCE_LOCATION}/.."
CVMFS_RESULT_LOCATION="$2"

rpm_infra_dirs="BUILD RPMS SOURCES SRPMS TMP"
rpm_src_dir="${CVMFS_SOURCE_LOCATION}/packaging/rpm"
spec_file="cvmfs-server-portals.spec"

# sanity checks
for d in docker-graphdriver minio; do
  [ -d ${ALL_SOURCE_LOCATION}/$d ] || die "sources $d missing"
done
for d in $rpm_infra_dirs; do
  [ ! -d ${CVMFS_RESULT_LOCATION}/${d} ] || die "build directory seems to be used before (${CVMFS_RESULT_LOCATION}/${d} exists)"
done
[ ! -f ${rpm_src_dir}/${spec_file} ] || die "source directory seemed to be built before $spec_file exists"

echo "preparing build environment in '${CVMFS_RESULT_LOCATION}'..."
for d in $rpm_infra_dirs; do
  mkdir ${CVMFS_RESULT_LOCATION}/${d}
done

echo "preparing sources in '${CVMFS_RESULT_LOCATION}/SOURCES'..."
charon_version=$(cat ${rpm_src_dir}/${spec_file} | grep charon_version | grep ^%define | awk '{print $3}')
charon_commitid=$(cd ${ALL_SOURCE_LOCATION}/docker-graphdriver && git rev-parse HEAD)
(${ALL_SOURCE_LOCATION}/docker-graphdriver && \
  git archive --format=tar --prefix=docker-graphdriver-1.1/ \
    -o ../../build/SOURCES/docker-graphdriver-${charon_version}.tar.gz HEAD)
minio_tag=$(cat ${rpm_src_dir}/${spec_file} | grep minio_tag | grep ^%define | awk '{print $3}')
minio_commitid=$(cd ${ALL_SOURCE_LOCATION}/minio && git rev-parse ${minio_tag})
(cd ${ALL_SOURCE_LOCATION}/minio && \
  git archive --format=tar --prefix=cvmfs-minio-${minio_tag}/ \
    -o ../../build/${minio_tag}.tar.gz ${minio_tag})

echo "Building!"
cd $CVMFS_RESULT_LOCATION
rpmbuild --define="_topdir $CVMFS_RESULT_LOCATION"        \
         --define="_tmppath ${CVMFS_RESULT_LOCATION}/TMP" \
	 --define="charon_commitid ${charon_commitid}"    \
	 --define="minio_commitid ${minio_commitid}"      \
         -ba $spec_file

