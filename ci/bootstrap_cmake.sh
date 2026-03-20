#!/usr/bin/env bash

# Bootstrap CMake to a sufficiently new version for building CernVM-FS.
# Downloads Kitware pre-built binaries for x86_64 and aarch64 on Linux;
# builds from source for other architectures.
#
# Usage:
#   ci/bootstrap_cmake.sh [OPTIONS]
# Options:
#   --install-dir <dir>     Override install location (default: externals_install*)
#   --cmake-version <ver>   CMake version to fetch  (default: 3.31.7,
#                           or env CMAKE_BOOTSTRAP_VERSION)
#   --min-version <ver>     Minimum acceptable version (default: 3.24.0,
#                           or env CMAKE_MIN_VERSION)
#   -h, --help              Show this help
#
# Logs go to stderr.  On success, prints exactly one line to stdout:
#   CVMFS_CMAKE_DIR=<path/to/dir/containing/cmake>
# Callers can eval that line to capture the path, e.g.:
#   eval "$(ci/bootstrap_cmake.sh)"

set -euo pipefail

die()  { echo "ERROR: $*" >&2; exit 1; }
log()  { echo "[bootstrap-cmake] $*" >&2; }
check_available() { command -v "$1" >/dev/null 2>&1; }

CMAKE_VERSION="${CMAKE_BOOTSTRAP_VERSION:-3.31.7}"
MIN_VERSION="${CMAKE_MIN_VERSION:-3.24.0}"
INSTALL_DIR_OVERRIDE=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --install-dir)    INSTALL_DIR_OVERRIDE="$2"; shift 2;;
    --cmake-version)  CMAKE_VERSION="$2";        shift 2;;
    --min-version)    MIN_VERSION="$2";          shift 2;;
    -h|--help) sed -n '3,17p' "$0" | sed 's/^# \{0,1\}//'; exit 0;;
    *) die "Unknown argument: $1";;
  esac
done

# ---------------------------------------------------------------------------
# Version helpers
# ---------------------------------------------------------------------------

# version_ge A B — succeeds (returns 0) when A >= B
version_ge() {
  local a="$1" b="$2"
  IFS='.' read -ra av <<< "$a"
  IFS='.' read -ra bv <<< "$b"
  local i
  for i in 0 1 2; do
    local an="${av[$i]:-0}"
    local bn="${bv[$i]:-0}"
    (( an > bn )) && return 0
    (( an < bn )) && return 1
  done
  return 0  # equal
}

cmake_installed_version() {
  "${1:-cmake}" --version 2>/dev/null \
    | head -1 \
    | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' \
    || true
}

# ---------------------------------------------------------------------------
# Check whether the system cmake already satisfies the requirement
# ---------------------------------------------------------------------------
if check_available cmake; then
  CURRENT_VER="$(cmake_installed_version cmake)"
  if [ -n "$CURRENT_VER" ] && version_ge "$CURRENT_VER" "$MIN_VERSION"; then
    log "cmake ${CURRENT_VER} >= ${MIN_VERSION} — no bootstrap needed"
    echo "CVMFS_CMAKE_DIR=$(dirname "$(command -v cmake)")"
    exit 0
  fi
  log "cmake ${CURRENT_VER:-unknown} < ${MIN_VERSION} — bootstrapping..."
else
  log "cmake not found — bootstrapping..."
fi

# ---------------------------------------------------------------------------
# Determine install location  (mirrors build_libfuse.sh logic)
# ---------------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
ARCH="$(uname -m)"

if [ -n "$INSTALL_DIR_OVERRIDE" ]; then
  EXTERNALS_INSTALL_LOCATION="$INSTALL_DIR_OVERRIDE"
else
  DISTRO=""
  if [ -f /etc/os-release ]; then
    # shellcheck disable=SC1091
    . /etc/os-release 2>/dev/null || true
    if [ -n "${PLATFORM_ID:-}" ]; then
      DISTRO="${PLATFORM_ID#*:}"           # e.g. "platform:el9" → "el9"
    else
      DISTRO="${ID:-}${VERSION_ID:-}"
      DISTRO="${DISTRO//\"/}"
    fi
  fi

  if [ -d "${REPO_ROOT}/externals_build" ] && [ -d "${REPO_ROOT}/externals_install" ]; then
    EXTERNALS_INSTALL_LOCATION="${REPO_ROOT}/externals_install"
  else
    EXTERNALS_INSTALL_LOCATION="${REPO_ROOT}/externals_install.${ARCH}"
    if [ -n "$DISTRO" ]; then
      EXTERNALS_INSTALL_LOCATION="${EXTERNALS_INSTALL_LOCATION}.${DISTRO}"
    fi
  fi
fi

mkdir -p "${EXTERNALS_INSTALL_LOCATION}"
CMAKE_BIN="${EXTERNALS_INSTALL_LOCATION}/bin/cmake"

# ---------------------------------------------------------------------------
# Short-circuit: already installed at target with a good-enough version
# ---------------------------------------------------------------------------
if [ -x "${CMAKE_BIN}" ]; then
  INSTALLED_VER="$(cmake_installed_version "${CMAKE_BIN}")"
  if [ -n "$INSTALLED_VER" ] && version_ge "$INSTALLED_VER" "$MIN_VERSION"; then
    log "cmake ${INSTALLED_VER} already present at ${EXTERNALS_INSTALL_LOCATION}/bin"
    echo "CVMFS_CMAKE_DIR=${EXTERNALS_INSTALL_LOCATION}/bin"
    exit 0
  fi
fi

log "Installing cmake ${CMAKE_VERSION} into ${EXTERNALS_INSTALL_LOCATION}"

KITWARE_BASE="https://github.com/Kitware/CMake/releases/download/v${CMAKE_VERSION}"
OS="$(uname -s)"

# ---------------------------------------------------------------------------
# Install from Kitware pre-built binary (Linux self-extracting shell script)
# ---------------------------------------------------------------------------
install_cmake_binary() {
  local arch_label="$1"    # e.g. x86_64, aarch64
  local platform_label="$2" # e.g. linux
  local url="${KITWARE_BASE}/cmake-${CMAKE_VERSION}-${platform_label}-${arch_label}.sh"
  local installer="${EXTERNALS_INSTALL_LOCATION}/_cmake_installer_$$.sh"

  log "Downloading ${url}"
  check_available curl || die "curl is required to download cmake"
  curl -fsSL "${url}" -o "${installer}" \
    || die "Failed to download cmake from ${url}"
  chmod +x "${installer}"

  log "Extracting into ${EXTERNALS_INSTALL_LOCATION}..."
  # --exclude-subdir installs bin/, lib/, share/ directly under the prefix
  "${installer}" --skip-license --prefix="${EXTERNALS_INSTALL_LOCATION}" --exclude-subdir
  rm -f "${installer}"
}

# ---------------------------------------------------------------------------
# Build cmake from source (fallback for exotic architectures / macOS)
# ---------------------------------------------------------------------------
build_cmake_from_source() {
  local url="${KITWARE_BASE}/cmake-${CMAKE_VERSION}.tar.gz"
  local build_dir
  build_dir="$(mktemp -d)"
  local tarball="${build_dir}/cmake-${CMAKE_VERSION}.tar.gz"
  local srcdir="${build_dir}/cmake-${CMAKE_VERSION}"

  log "Downloading source ${url}"
  check_available curl || die "curl is required to download cmake source"
  curl -fsSL "${url}" -o "${tarball}" \
    || die "Failed to download cmake source from ${url}"
  tar -xzf "${tarball}" -C "${build_dir}"

  local jobs
  jobs="$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo 1)"
  log "Building cmake from source with ${jobs} parallel jobs (this may take a while)..."

  cd "${srcdir}"
  # Disable OpenSSL to avoid dependency issues; cmake can use its own curl/openssl
  ./bootstrap \
    --prefix="${EXTERNALS_INSTALL_LOCATION}" \
    --parallel="${jobs}" \
    -- -DCMAKE_USE_OPENSSL=OFF \
    || die "cmake bootstrap script failed"
  make -j"${jobs}" || die "cmake source build failed"
  make install     || die "cmake install failed"
  cd - >/dev/null
  rm -rf "${build_dir}"
}

# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------
case "${OS}" in
  Linux)
    case "${ARCH}" in
      x86_64)         install_cmake_binary "x86_64"  "linux" ;;
      aarch64|arm64)  install_cmake_binary "aarch64" "linux" ;;
      *)
        log "No Kitware binary for ${ARCH} — building from source"
        build_cmake_from_source
        ;;
    esac
    ;;
  Darwin)
    # Homebrew is the easiest path on macOS; fall back to source build
    if check_available brew; then
      log "macOS + brew detected — running: brew install cmake"
      brew install cmake >/dev/null
      # Homebrew installs cmake into its prefix; find it
      BREW_CMAKE="$(brew --prefix cmake 2>/dev/null)/bin/cmake"
      if [ -x "${BREW_CMAKE}" ]; then
        log "cmake installed via brew at $(dirname "${BREW_CMAKE}")"
        echo "CVMFS_CMAKE_DIR=$(dirname "${BREW_CMAKE}")"
        exit 0
      fi
    fi
    log "No brew or brew cmake path not found — building from source"
    build_cmake_from_source
    ;;
  *)
    log "Unknown OS '${OS}' — building cmake from source"
    build_cmake_from_source
    ;;
esac

# ---------------------------------------------------------------------------
# Verify
# ---------------------------------------------------------------------------
[ -x "${CMAKE_BIN}" ] \
  || die "cmake installation failed: ${CMAKE_BIN} not found after install"

INSTALLED_VER="$(cmake_installed_version "${CMAKE_BIN}")"
log "cmake ${INSTALLED_VER} installed successfully"
echo "CVMFS_CMAKE_DIR=${EXTERNALS_INSTALL_LOCATION}/bin"
