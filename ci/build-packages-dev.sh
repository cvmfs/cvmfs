#!/usr/bin/env sh
#
# build-packages-dev.sh – Rebuild RPM packages from an existing build directory
#
# Unlike build_package.sh / ci/cvmfs/rpm.sh this script skips the full cmake
# configure and avoids extracting a fresh source tarball into the rpmbuild
# tree.  Instead it:
#
#   1. Runs an incremental "cmake --build" inside the already-configured build
#      directory (only changed translation units are recompiled).
#   2. Invokes rpmbuild with --short-circuit -bb to skip %%prep and %%build and
#      jump straight to %%install + packaging.  The %%install stage does
#      "make install DESTDIR=..." against the pre-built artefacts.
#
# Requirements:
#   - A full build must have been completed at least once with build_package.sh
#     (or equivalent) so that the rpmbuild work directory contains:
#       <result-dir>/cvmfs-universal.spec
#       <result-dir>/BUILD/cvmfs-<version>/   (cmake build directory)
#
# Usage:
#   build-packages-dev.sh <source-dir> <result-dir>
#
# Example (replicating what Dockerfile.gateway's builder stage does, but
# incrementally after the first full run):
#   build-packages-dev.sh /home/sftnight/cvmfs /home/sftnight/buildpackage
#

set -e

SCRIPT_LOCATION=$(cd "$(dirname "$0")"; pwd)
. "${SCRIPT_LOCATION}/common.sh"

if [ $# -lt 2 ]; then
  echo "Usage: $0 <CernVM-FS source directory> <existing build result location>"
  echo ""
  echo "  <source directory>  – root of the CernVM-FS source tree"
  echo "  <result location>   – directory that was used as the output of a"
  echo "                        previous full build_package.sh / rpm.sh run"
  echo "                        (must contain cvmfs-universal.spec and"
  echo "                         BUILD/cvmfs-<version>/CMakeCache.txt)"
  exit 1
fi

CVMFS_SOURCE_LOCATION="$1"
CVMFS_RESULT_LOCATION="$2"

spec_file="cvmfs-universal.spec"

# Sanity checks ---------------------------------------------------------------

[ -f "${CVMFS_RESULT_LOCATION}/${spec_file}" ] || \
  die "No ${spec_file} in '${CVMFS_RESULT_LOCATION}'. Run a full build first:
  ci/build_package.sh ${CVMFS_SOURCE_LOCATION} ${CVMFS_RESULT_LOCATION} cvmfs"

cmake_build_dir=$(find "${CVMFS_RESULT_LOCATION}/BUILD" -maxdepth 2 \
  -name CMakeCache.txt 2>/dev/null | head -1 | xargs -r dirname)

[ -n "${cmake_build_dir}" ] || \
  die "Cannot locate CMakeCache.txt under '${CVMFS_RESULT_LOCATION}/BUILD'.
  Run a full build first so the cmake build tree is in place."

# Prepend bootstrapped cmake / go to PATH the same way rpm.sh does -----------

cvmfs_version="$(get_cvmfs_version_from_cmake "${CVMFS_SOURCE_LOCATION}")"
cvmfs_prerelease="$(get_cvmfs_prerelease_from_cmake "${CVMFS_SOURCE_LOCATION}")"

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
[ -n "${_distro}" ] && _install_dir="${_install_dir}.${_distro}"

_cmake_dir="${CVMFS_CMAKE_DIR:-}"
[ -z "${_cmake_dir}" ] && [ -x "${_install_dir}/bin/cmake" ] && \
  _cmake_dir="${_install_dir}/bin"
_go_dir="${CVMFS_GO_DIR:-}"
[ -z "${_go_dir}" ] && [ -x "${_install_dir}/go/bin/go" ] && \
  _go_dir="${_install_dir}/go/bin"

_prepend="${_cmake_dir:+${_cmake_dir}:}${_go_dir}"
[ -n "${_prepend}" ] && export PATH="${_prepend}:${PATH}"

# Incremental build -----------------------------------------------------------

_nproc=$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 4)

echo "==> Incremental cmake build in '${cmake_build_dir}' (${_nproc} jobs)..."
cmake --build "${cmake_build_dir}" --parallel "${_nproc}"

# Repackage (skip %%prep + %%build, run %%install + %%files only) -------------

# Remove stale RPMS / BUILDROOT so rpmbuild starts packaging from a clean slate.
rm -rf "${CVMFS_RESULT_LOCATION}/RPMS" \
       "${CVMFS_RESULT_LOCATION}/BUILDROOT"

echo "==> Repackaging with rpmbuild --short-circuit -bb ..."
cd "${CVMFS_RESULT_LOCATION}"
rpmbuild --define="_topdir ${CVMFS_RESULT_LOCATION}" \
         --short-circuit -bb \
         "${spec_file}"

echo "==> Done. RPMs written to ${CVMFS_RESULT_LOCATION}/RPMS/"
