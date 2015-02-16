#!/bin/sh

#
# This script builds the RPM packages of CernVM-FS.
#

set -e

SCRIPT_LOCATION=$(cd "$(dirname "$0")"; pwd)
. ${SCRIPT_LOCATION}/common.sh

if [ $# -lt 2 ]; then
  echo "Usage: $0 <CernVM-FS source directory> <build result location> [<nightly build number>]"
  echo "This script builds CernVM-FS RPM packages"
  exit 1
fi

CVMFS_SOURCE_LOCATION="$1"
CVMFS_RESULT_LOCATION="$2"
CVMFS_NIGHTLY_BUILD_NUMBER="${3-0}"

rpm_infra_dirs="BUILD RPMS SOURCES SRPMS TMP"
rpm_src_dir="${CVMFS_SOURCE_LOCATION}/packaging/rpm"
spec_file="cvmfs-universal.spec"

# sanity checks
for d in $rpm_infra_dirs; do
  [ ! -d ${CVMFS_RESULT_LOCATION}/${d} ] || die "build directory seems to be used before (${CVMFS_RESULT_LOCATION}/${d} exists)"
done
[ ! -f ${CVMFS_SOURCE_LOCATION}/${spec_file} ] || die "source directory seemed to be built before (${CVMFS_SOURCE_LOCATION}/$spec_file exists)"

# retrieve the upstream version string from CVMFS
cvmfs_version="$(get_cvmfs_version_from_cmake $CVMFS_SOURCE_LOCATION)"
echo "detected upstream version: $cvmfs_version"

echo "preparing build environment in '${CVMFS_RESULT_LOCATION}'..."
for d in $rpm_infra_dirs; do
  mkdir ${CVMFS_RESULT_LOCATION}/${d}
done

# copy RPM spec file and SELinux module files in place and cd there
cp ${rpm_src_dir}/$spec_file $CVMFS_RESULT_LOCATION
cp ${rpm_src_dir}/cvmfs.te \
   ${rpm_src_dir}/cvmfs.fc \
   $CVMFS_RESULT_LOCATION/SOURCES
# cp $tarball $packagedir/SOURCES || exit 5
cd  $CVMFS_RESULT_LOCATION

# generate the release tag for either a nightly build or a release
# (for the nightly build this requires some changes in the spec file)
if [ $CVMFS_NIGHTLY_BUILD_NUMBER -gt 0 ]; then
  git_hash="$(get_cvmfs_git_revision $CVMFS_SOURCE_LOCATION)"
  build_tag="git-${git_hash}"
  nightly_tag="0.${CVMFS_NIGHTLY_BUILD_NUMBER}.${git_hash}git"

  echo "creating nightly build '$nightly_tag'"
  sed -i -e "s/^Release: .*/Release: ${nightly_tag}%{?dist}/" $spec_file
  sed -i -e "s/\(^Source0: .*\)%{version}/\1${build_tag}/"    $spec_file
  sed -i -e "s/^%setup -q/%setup -q -n cvmfs-${build_tag}/"   $spec_file
else
  echo "creating release: $cvmfs_version"
fi

echo "building..."
rpmbuild --define="_topdir $CVMFS_RESULT_LOCATION"        \
         --define="_tmppath ${CVMFS_RESULT_LOCATION}/TMP" \
         -ba $spec_file
