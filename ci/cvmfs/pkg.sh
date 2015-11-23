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

pkg_basedir=${CVMFS_SOURCE_LOCATION}/packaging/mac
pmdoc_template=${pkg_basedir}/cvmfs.pmdoc.template
pmdoc=${pkg_basedir}/cvmfs.pmdoc
pkg_install_dir=${CVMFS_RESULT_LOCATION}/CVMFS_Package

# sanity checks
[ ! -d $pmdoc ]           || die "source tree seems to be built before ($pmdoc exists)"
[ ! -d $pkg_install_dir ] || die "build directory was used before ($pkg_install_dir exists)"

echo "building CernVM-FS in '$CVMFS_RESULT_LOCATION' from '$CVMFS_SOURCE_LOCATION'"
cd $CVMFS_RESULT_LOCATION
cmake -DCMAKE_INSTALL_PREFIX:PATH=/usr \
      -DBUILD_SERVER=no                \
      -DBUILD_SERVER_DEBUG=no          \
      -DBUILD_UNITTESTS=no             \
      $CVMFS_SOURCE_LOCATION
make

echo "Looking for PackageMaker ..."
package_maker=""
if which packagemaker > /dev/null 2>&1; then
  package_maker="$(which packagemaker)"
else
  package_maker=/Applications/PackageMaker.app/Contents/MacOS/PackageMaker
fi
[ -x $package_maker ] || die "PackageMaker not found/executable"
echo "PackageMaker found in $package_maker"

# retrieve the upstream version string from CVMFS
cvmfs_version="$(get_cvmfs_version_from_cmake $CVMFS_SOURCE_LOCATION)"
echo "detected upstream version: $cvmfs_version"

# generate the release tag for either a nightly build or a release
cvmfs_build_tag="cvmfs-${cvmfs_version}"
if [ $CVMFS_NIGHTLY_BUILD_NUMBER -gt 0 ]; then
  git_hash="$(get_cvmfs_git_revision $CVMFS_SOURCE_LOCATION)"
  cvmfs_build_tag="${cvmfs_build_tag}.0-${CVMFS_NIGHTLY_BUILD_NUMBER}-git-${git_hash}"
  echo "creating nightly build '$cvmfs_build_tag'"
else
  echo "creating release: $cvmfs_build_tag"
fi

echo "Installing cvmfs to $pkg_install_dir ..."
make install DESTDIR=$pkg_install_dir || die "failed to install"

echo "Creating $pmdoc from $pmdoc_template ..."
output_package="${CVMFS_RESULT_LOCATION}/${cvmfs_build_tag}.pkg"
cp -a ${pmdoc_template} ${pmdoc}
sed -i -e "s/@PackageOutput@/$(echo $output_package | sed -e "s,/,\\\\/,g")/g" $pmdoc/*.xml
sed -i -e "s/@PackageInput@/$(echo $pkg_install_dir | sed -e "s,/,\\\\/,g")/g" $pmdoc/*.xml

echo "Creating package $output_package ..."
cd $pkg_basedir
$package_maker --doc $pmdoc       \
               --verbose          \
               --root-volume-only \
               --out $output_package || die "Package creation failed!"
cd ${CVMFS_RESULT_LOCATION}

# generating package map section for specific platform
if [ ! -z $CVMFS_CI_PLATFORM_LABEL ]; then
  echo "generating package map section for ${CVMFS_CI_PLATFORM_LABEL}..."
  generate_package_map "$CVMFS_CI_PLATFORM_LABEL"                            \
                       "$(basename $(find . -regex '.*cvmfs-[0-9].*\.pkg'))" \
                       ""                                                    \
                       ""                                                    \
                       ""                                                    \
                       ""
fi
