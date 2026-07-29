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
echo "detected upstream version: ${cvmfs_version}${cvmfs_prerelease}"

# generate the release tag for either a nightly build or a release
if [ $CVMFS_NIGHTLY_BUILD_NUMBER -gt 0 ]; then
  git_hash="$(get_cvmfs_git_revision $CVMFS_SOURCE_LOCATION)"
  cvmfs_version="${cvmfs_version}~0.${CVMFS_NIGHTLY_BUILD_NUMBER}git${git_hash}"
  echo "creating nightly build '$cvmfs_version'"
else
  cvmfs_version="${cvmfs_version}"
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
cp -R --dereference ${CVMFS_SOURCE_LOCATION}/CITATION.cff       \
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
# libfuse2 packaging is disabled: libfuse-dev is not available in the build
# environment and libfuse3 is used on all supported platforms.
BUILD_LIBFUSE2=no
if [ "${BUILD_LIBFUSE2}" = "yes" ]; then
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
# debuild sanitises PATH, so custom tools installed by bootstrap_cmake.sh /
# bootstrap_golang.sh must be injected explicitly via --prepend-path.
# Compute the externals install directory once; both cmake and go use it.
# Mirror bootstrap_cmake.sh/bootstrap_golang.sh: prefer CVMFS_EXTERNALS_PREFIX,
# fall back to the source tree root (which is their REPO_ROOT fallback).
_base="${CVMFS_EXTERNALS_PREFIX:-${CVMFS_SOURCE_LOCATION}}"
_arch="$(uname -m)"
_distro=""
if [ -f /etc/os-release ]; then
  # shellcheck disable=SC1091
  . /etc/os-release 2>/dev/null || true
  if [ -n "${PLATFORM_ID:-}" ]; then
    _distro="${PLATFORM_ID#*:}"
  else
    _distro="$(echo "${ID:-}${VERSION_ID:-}" | tr -d '"')"
  fi
fi
_install_dir="${_base}/externals_install.${_arch}"
if [ -n "$_distro" ]; then
  _install_dir="${_install_dir}.${_distro}"
fi

# cmake: CVMFS_CMAKE_DIR (explicit) → auto-detected from prefix
_cmake_dir="${CVMFS_CMAKE_DIR:-}"
if [ -z "$_cmake_dir" ] && [ -x "${_install_dir}/bin/cmake" ]; then
  _cmake_dir="${_install_dir}/bin"
fi

# go: CVMFS_GO_DIR (explicit) → auto-detected from prefix → /usr/local/go/bin (fallback)
_go_dir="${CVMFS_GO_DIR:-}"
if [ -z "$_go_dir" ] && [ -x "${_install_dir}/go/bin/go" ]; then
  _go_dir="${_install_dir}/go/bin"
fi
if [ -z "$_go_dir" ]; then
  _go_dir="/usr/local/go/bin"
fi

# Combine into one --prepend-path: debuild only honours the last occurrence.
_prepend_path="${_go_dir}"
if [ -n "$_cmake_dir" ]; then
  _prepend_path="${_cmake_dir}:${_prepend_path}"
fi

DEB_BUILD_OPTIONS=parallel=$cpu_cores debuild ${DEBUILD_ARGS} \
  --prepend-path="${_prepend_path}" \
  -e  CVMFS_EXTERNALS_PREFIX="${CVMFS_EXTERNALS_PREFIX}" \
  -e  CMAKE_CXX_COMPILER_LAUNCHER="${CMAKE_CXX_COMPILER_LAUNCHER}" \
  -e  GOPROXY="${GOPROXY}" \
  --check-dirname-level 0 \
  -us -uc
cd ${CVMFS_RESULT_LOCATION}

# generating package map section for specific platform
if [ ! -z $CVMFS_CI_PLATFORM_LABEL ]; then
  echo "generating package map section for ${CVMFS_CI_PLATFORM_LABEL}..."
  # Return the first .deb matching $1, or empty if none — avoids
  # `basename: missing operand` when an optional package wasn't built.
  _find_deb() {
    find . -maxdepth 2 -name "$1" -printf '%f\n' 2>/dev/null | head -n1
  }
  generate_package_map "$CVMFS_CI_PLATFORM_LABEL"      \
                       "$(_find_deb 'cvmfs_*.deb')"          \
                       "$(_find_deb 'cvmfs-server*.deb')"    \
                       "$(_find_deb 'cvmfs-dev*.deb')"       \
                       "$(_find_deb 'cvmfs-unittests*.deb')" \
                       "$CVMFS_CONFIG_PACKAGE"               \
                       "$(_find_deb 'cvmfs-shrinkwrap*.deb')"\
                       ""                                    \
                       "$(_find_deb 'cvmfs-fuse3*.deb')"     \
                       "$(_find_deb 'cvmfs-gateway*.deb')"   \
                       "$(_find_deb 'cvmfs-libs*.deb')"
fi

# clean up the source tree
echo "cleaning up..."
rm -fR $copied_source
