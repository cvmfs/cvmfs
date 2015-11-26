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
pkg_install_dir=${CVMFS_RESULT_LOCATION}/CVMFS_Package

# sanity checks
[ ! -d $pkg_install_dir ] || die "build directory was used before ($pkg_install_dir exists)"

# retrieve the upstream version string from CVMFS
cvmfs_version="$(get_cvmfs_version_from_cmake $CVMFS_SOURCE_LOCATION)"
echo "detected upstream version: $cvmfs_version"

# check if there is an OpenSSL installed via brew
OPENSSL_INCLUDE=""
brew_openssl_keg=/usr/local/opt/openssl/include
if [ -d $brew_openssl_keg ]; then
  echo "using homebrew'ed OpenSSL at $brew_openssl_keg"
  OPENSSL_INCLUDE="-DOPENSSL_INCLUDE_DIR=$brew_openssl_keg"
fi

echo "building CernVM-FS $cvmfs_version in '$CVMFS_RESULT_LOCATION' from '$CVMFS_SOURCE_LOCATION'"
cd $CVMFS_RESULT_LOCATION
cmake -DCMAKE_INSTALL_PREFIX:PATH=/usr/local \
      -DBUILD_SERVER=no                      \
      -DBUILD_SERVER_DEBUG=no                \
      -DBUILD_UNITTESTS=no                   \
      $OPENSSL_INCLUDE                       \
      $CVMFS_SOURCE_LOCATION
make -j $(get_number_of_cpu_cores)

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
