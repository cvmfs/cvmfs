#!/bin/sh

#
# This script builds the Mac OS X package of CernVM-FS.
#

set -e

SCRIPT_LOCATION=$(cd "$(dirname "$0")"; pwd)
. ${SCRIPT_LOCATION}/../common.sh

if [ $# -lt 2 ]; then
  echo "Usage: $0 <CernVM-FS source directory> <build result location> [<nightly build number>]"
  echo "This script builds CernVM-FS OS X package"
  exit 1
fi

CVMFS_SOURCE_LOCATION="$1"
CVMFS_RESULT_LOCATION="$2"
CVMFS_NIGHTLY_BUILD_NUMBER="${3-0}"

# this uniquely identifies a package in OSX for example to distinguish between
# fresh installations and updates. Keep that consistent!
# Note: It has changed with the transition from PackageMaker to pkgbuild and the
#       preinstall script takes care of checking for outdated versions.
#       Previously it was ch.cern.cvmfs.CVMFS_Package.pkg
CVMFS_CORE_PKG_IDENTIFIER="ch.cern.cvmfs.pkg.Core"
CVMFS_PKG_IDENTIFIER="ch.cern.cvmfs.pkg"
CVMFS_INSTALL_PREFIX="/usr/local"

pkg_basedir=${CVMFS_SOURCE_LOCATION}/packaging/mac
pkg_install_dir=${CVMFS_RESULT_LOCATION}/pkg_install
pkg_resource_dir=${CVMFS_RESULT_LOCATION}/pkg_resources
pkg_build_dir=${CVMFS_RESULT_LOCATION}/pkg_result

# sanity checks
[ ! -d $pkg_install_dir ]           || die "build directory was used before ($pkg_install_dir exists)"
[ ! -d $pkg_build_dir ]             || die "build directory was used before ($pkg_build_dir exists)"
[ ! -d $pkg_resource_dir ]          || die "build directory was used before ($pkg_resource_dir exists)"
which pkgbuild     > /dev/null 2>&1 || die "didn't find 'pkgbuild' utility"
which productbuild > /dev/null 2>&1 || die "didn't find 'productbuild' utility"
which tiffutil     > /dev/null 2>&1 || die "didn't find 'tiffutil' utility"

# setup environment
mkdir -p $pkg_install_dir
mkdir -p $pkg_build_dir
mkdir -p $pkg_resource_dir

# retrieve the upstream version string from CVMFS
cvmfs_version="$(get_cvmfs_version_from_cmake $CVMFS_SOURCE_LOCATION)"
echo "detected upstream version: $cvmfs_version"

echo "building CernVM-FS $cvmfs_version in '$CVMFS_RESULT_LOCATION' from '$CVMFS_SOURCE_LOCATION'"
cd $CVMFS_RESULT_LOCATION
cmake -DCMAKE_INSTALL_PREFIX:PATH=$CVMFS_INSTALL_PREFIX \
      -DBUILD_SERVER=no                                 \
      -DBUILD_SERVER_DEBUG=no                           \
      -DBUILD_UNITTESTS=no                              \
      $OPENSSL_INCLUDE                                  \
      $CVMFS_SOURCE_LOCATION
make -j $(get_number_of_cpu_cores)

# generate the release tag for either a nightly build or a release
cvmfs_build_tag="cvmfs-${cvmfs_version}"
if [ $CVMFS_NIGHTLY_BUILD_NUMBER -gt 0 ]; then
  git_hash="$(get_cvmfs_git_revision $CVMFS_SOURCE_LOCATION)"
  cvmfs_build_tag="${cvmfs_build_tag}-0.${CVMFS_NIGHTLY_BUILD_NUMBER}-git-${git_hash}"
  echo "creating nightly build '$cvmfs_build_tag'"
else
  echo "creating release: $cvmfs_build_tag"
fi

echo "Installing cvmfs to $pkg_install_dir ..."
make install DESTDIR=$pkg_install_dir || die "failed to install"

echo "packaging up CernVM-FS ${cvmfs_version}..."
cvmfs_package="${pkg_build_dir}/${cvmfs_build_tag}-core.pkg"
pkgbuild --root             ${pkg_install_dir}/${CVMFS_INSTALL_PREFIX} \
         --scripts          ${pkg_basedir}/scripts                     \
         --identifier       $CVMFS_CORE_PKG_IDENTIFIER                 \
         --version          $cvmfs_version                             \
         --install-location $CVMFS_INSTALL_PREFIX                      \
         $cvmfs_package

echo "generating product package build environment..."
cvmfs_dist_file="${pkg_build_dir}/cvmfs.dist"
expand_template ${pkg_basedir}/cvmfs.dist.template > $cvmfs_dist_file
cp ${pkg_basedir}/resources/* ${pkg_resource_dir}
tiffutil -cat ${pkg_resource_dir}/cvmfs.png    \
              ${pkg_resource_dir}/cvmfs@2x.png \
         -out ${pkg_resource_dir}/cvmfs_retina.tif

echo "packaging up the production CernVM-FS package..."
product_package="${pkg_build_dir}/${cvmfs_build_tag}.pkg"
productbuild --distribution $cvmfs_dist_file  \
             --resources    $pkg_resource_dir \
             --package-path $pkg_build_dir    \
             $product_package

echo "removing intermediate CernVM-FS package..."
rm -f $cvmfs_package

# generating package map section for specific platform
if [ ! -z $CVMFS_CI_PLATFORM_LABEL ]; then
  echo "generating package map section for ${CVMFS_CI_PLATFORM_LABEL}..."
  generate_package_map "$CVMFS_CI_PLATFORM_LABEL" \
                       "${cvmfs_build_tag}.pkg"   \
                       ""                         \
                       ""                         \
                       ""                         \
                       ""
fi
