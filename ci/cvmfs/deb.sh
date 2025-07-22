#!/bin/sh

#
# This script builds the debian packages of CernVM-FS.
#

set -e

SCRIPT_LOCATION=$(cd "$(dirname "$0")"; pwd)
. ${SCRIPT_LOCATION}/../common.sh

if [ $# -lt 2 ]; then
  echo "Usage: $0 <CernVM-FS source directory> <build result location> [<nightly build number>]"
  echo "This script builds CernVM-FS debian packages"
  exit 1
fi

CVMFS_SOURCE_LOCATION="$1"
CVMFS_RESULT_LOCATION="$2"
CVMFS_NIGHTLY_BUILD_NUMBER="${3-0}"

CVMFS_CONFIG_PACKAGE="cvmfs-config-default_2.2-1_all.deb"

# retrieve the upstream version string from CVMFS
cvmfs_version="$(get_cvmfs_version_from_cmake $CVMFS_SOURCE_LOCATION)"
cvmfs_prerelease="$(get_cvmfs_prerelease_from_cmake $CVMFS_SOURCE_LOCATION)"
echo "detected upstream version: ${cvmfs_version}${cvmfs_preelease}"

# generate the release tag for either a nightly build or a release
if [ $CVMFS_NIGHTLY_BUILD_NUMBER -gt 0 ]; then
  git_hash="$(get_cvmfs_git_revision $CVMFS_SOURCE_LOCATION)"
  cvmfs_version="${cvmfs_version}~0.${CVMFS_NIGHTLY_BUILD_NUMBER}git${git_hash}"
  echo "creating nightly build '$cvmfs_version'"
else
  cvmfs_version="${cvmfs_version}-1"
fi
cvmfs_version="${cvmfs_version}+$(lsb_release -si | tr [:upper:] [:lower:])"
cvmfs_version="${cvmfs_version}$(lsb_release -sr)"
cvmfs_version="${cvmfs_version}${cvmfs_prerelease}"

echo "creating release: $cvmfs_version"

# copy the entire source tree into a working directory
echo "copying source into workspace..."
mkdir -p $CVMFS_RESULT_LOCATION
copied_source="${CVMFS_RESULT_LOCATION}/wd_src"
[ ! -d $copied_source ] || die "build directory is not empty"
mkdir -p $copied_source
cp -R --dereference ${CVMFS_SOURCE_LOCATION}/AUTHORS            \
                    ${CVMFS_SOURCE_LOCATION}/CMakeLists.txt     \
                    ${CVMFS_SOURCE_LOCATION}/COPYING            \
                    ${CVMFS_SOURCE_LOCATION}/ChangeLog          \
                    ${CVMFS_SOURCE_LOCATION}/INSTALL            \
                    ${CVMFS_SOURCE_LOCATION}/README.md          \
                    ${CVMFS_SOURCE_LOCATION}/add-ons            \
                    ${CVMFS_SOURCE_LOCATION}/cmake              \
                    ${CVMFS_SOURCE_LOCATION}/cvmfs              \
                    ${CVMFS_SOURCE_LOCATION}/doc                \
                    ${CVMFS_SOURCE_LOCATION}/externals          \
                    ${CVMFS_SOURCE_LOCATION}/gateway            \
                    ${CVMFS_SOURCE_LOCATION}/snapshotter        \
                    ${CVMFS_SOURCE_LOCATION}/mount              \
                    ${CVMFS_SOURCE_LOCATION}/test               \
                    ${CVMFS_SOURCE_LOCATION}/ducc               \
                    $copied_source


# produce the debian package
echo "copy packaging meta information and get in place..."
cp -r ${CVMFS_SOURCE_LOCATION}/packaging/debian/cvmfs ${copied_source}/debian
cd $copied_source

. /etc/os-release
VERSION_NUMBER=$(echo ${VERSION_ID} | tr -d '.')
BUILD_LIBFUSE2=yes
if [ "$ID" = "ubuntu" ] && [ ${VERSION_NUMBER} -ge 2504 ]; then
  BUILD_LIBFUSE2=no
fi
if [ "$ID" = "debian" ] && [ ${VERSION_NUMBER} -ge 13 ]; then
  BUILD_LIBFUSE2=no
fi
if [ "${BUILD_LIBUFSE2}" = "yes" ]; then
  sed -i -e "s/^#BUILD_LIBFUSE2//g" debian/control
  sed -i -e "s/^#BUILD_LIBFUSE2/BUILD_LIBFUSE2/g" debian/rules
fi


cpu_cores=$(get_number_of_cpu_cores)
echo "do the build (with $cpu_cores cores)..."
dch -v $cvmfs_version -M "bumped upstream version number"
# -us -uc == skip signing
DEBUILD_ARGS=""
if [ x"$CVMFS_LINT_PKG" = x ]; then
  DEBUILD_ARGS="--no-lintian"
fi
DEB_BUILD_OPTIONS=parallel=$cpu_cores debuild ${DEBUILD_ARGS} --prepend-path=/usr/local/go/bin \
  -e  CVMFS_EXTERNALS_PREFIX="${CVMFS_EXTERNALS_PREFIX}" \
  -e  CMAKE_CXX_COMPILER_LAUNCHER="${CMAKE_CXX_COMPILER_LAUNCHER}" \
  --check-dirname-level 0 \
  -us -uc
cd ${CVMFS_RESULT_LOCATION}

# generating package map section for specific platform
if [ ! -z $CVMFS_CI_PLATFORM_LABEL ]; then
  echo "generating package map section for ${CVMFS_CI_PLATFORM_LABEL}..."
  generate_package_map "$CVMFS_CI_PLATFORM_LABEL"                           \
                       "$(basename $(find . -name 'cvmfs_*.deb'))"          \
                       "$(basename $(find . -name 'cvmfs-server*.deb'))"    \
                       "$(basename $(find . -name 'cvmfs-dev*.deb'))"       \
                       "$(basename $(find . -name 'cvmfs-unittests*.deb'))" \
                       "$CVMFS_CONFIG_PACKAGE"                              \
                       "$(basename $(find . -name 'cvmfs-shrinkwrap*.deb'))"\
                       ""                                                   \
                       "$(basename $(find . -name 'cvmfs-fuse3*.deb'))"     \
                       "$(basename $(find . -name 'cvmfs-gateway*.deb'))"   \
                       "$(basename $(find . -name 'cvmfs-libs*.deb'))"
fi

# clean up the source tree
echo "cleaning up..."
rm -fR $copied_source
