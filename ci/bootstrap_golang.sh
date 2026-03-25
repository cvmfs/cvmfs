#!/usr/bin/env bash

# Bootstrap Go to a sufficiently new version for building CernVM-FS.
# Downloads pre-built binaries from go.dev for Linux and macOS (x86_64/aarch64).
#
# Usage:
#   ci/bootstrap_golang.sh [OPTIONS]
# Options:
#   --install-dir <dir>    Override install location (default: externals_install*)
#   --go-version <ver>     Go version to fetch (default: 1.24.2,
#                          or env GO_BOOTSTRAP_VERSION)
#   --min-version <ver>    Minimum acceptable version (default: 1.24.0,
#                          or env GO_MIN_VERSION)
#   -h, --help             Show this help
#
# Logs go to stderr.  On success, prints exactly one line to stdout:
#   CVMFS_GO_DIR=<path/to/dir/containing/go>
# Callers can eval that line to capture the path, e.g.:
#   eval "$(ci/bootstrap_golang.sh)"

set -euo pipefail

die()  { echo "ERROR: $*" >&2; exit 1; }
log()  { echo "[bootstrap-golang] $*" >&2; }
check_available() { command -v "$1" >/dev/null 2>&1; }

GO_VERSION="${GO_BOOTSTRAP_VERSION:-1.25.8}"
MIN_VERSION="${GO_MIN_VERSION:-1.24.0}"
INSTALL_DIR_OVERRIDE=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --install-dir)  INSTALL_DIR_OVERRIDE="$2"; shift 2;;
    --go-version)   GO_VERSION="$2";           shift 2;;
    --min-version)  MIN_VERSION="$2";          shift 2;;
    -h|--help) sed -n '3,20p' "$0" | sed 's/^# \{0,1\}//'; exit 0;;
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

go_installed_version() {
  "${1:-go}" version 2>/dev/null \
    | grep -oE 'go[0-9]+\.[0-9]+(\.[0-9]+)?' \
    | head -1 \
    | sed 's/^go//' \
    || true
}

# ---------------------------------------------------------------------------
# Check whether the system go already satisfies the requirement
# ---------------------------------------------------------------------------
if check_available go; then
  CURRENT_VER="$(go_installed_version go)"
  if [ -n "$CURRENT_VER" ] && version_ge "$CURRENT_VER" "$MIN_VERSION"; then
    log "go ${CURRENT_VER} >= ${MIN_VERSION} — no bootstrap needed"
    echo "CVMFS_GO_DIR=$(dirname "$(command -v go)")"
    exit 0
  fi
  log "go ${CURRENT_VER:-unknown} < ${MIN_VERSION} — bootstrapping..."
else
  log "go not found — bootstrapping..."
fi

# ---------------------------------------------------------------------------
# Determine install location (mirrors bootstrap_cmake.sh logic)
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

  # CVMFS_EXTERNALS_PREFIX overrides the base directory; otherwise fall back to
  # the repo root.  The install path is always the arch/distro-suffixed form.
  _BASE="${CVMFS_EXTERNALS_PREFIX:-${REPO_ROOT}}"
  EXTERNALS_INSTALL_LOCATION="${_BASE}/externals_install.${ARCH}"
  if [ -n "$DISTRO" ]; then
    EXTERNALS_INSTALL_LOCATION="${EXTERNALS_INSTALL_LOCATION}.${DISTRO}"
  fi
fi

mkdir -p "${EXTERNALS_INSTALL_LOCATION}"
# go.dev tarballs always extract into a 'go/' subdirectory
GO_BIN="${EXTERNALS_INSTALL_LOCATION}/go/bin/go"

# ---------------------------------------------------------------------------
# Short-circuit: already installed at target with a good-enough version
# ---------------------------------------------------------------------------
if [ -x "${GO_BIN}" ]; then
  INSTALLED_VER="$(go_installed_version "${GO_BIN}")"
  if [ -n "$INSTALLED_VER" ] && version_ge "$INSTALLED_VER" "$MIN_VERSION"; then
    log "go ${INSTALLED_VER} already present at ${EXTERNALS_INSTALL_LOCATION}/go/bin"
    echo "CVMFS_GO_DIR=${EXTERNALS_INSTALL_LOCATION}/go/bin"
    exit 0
  fi
fi

log "Installing go ${GO_VERSION} into ${EXTERNALS_INSTALL_LOCATION}"

OS="$(uname -s)"

# Map uname arch to Go's arch naming
go_arch() {
  case "$1" in
    x86_64)        echo "amd64" ;;
    aarch64|arm64) echo "arm64" ;;
    *) die "Unsupported architecture for Go pre-built binary: $1" ;;
  esac
}

# ---------------------------------------------------------------------------
# Install from go.dev pre-built tarball
# ---------------------------------------------------------------------------
install_go_binary() {
  local os_label="$1"    # e.g. linux, darwin
  local arch_label="$2"  # e.g. amd64, arm64
  local url="https://go.dev/dl/go${GO_VERSION}.${os_label}-${arch_label}.tar.gz"
  local tarball="${EXTERNALS_INSTALL_LOCATION}/_go_tarball_$$.tar.gz"

  log "Downloading ${url}"
  check_available curl || die "curl is required to download Go"
  curl -fsSL "${url}" -o "${tarball}" \
    || die "Failed to download Go from ${url}"

  log "Extracting into ${EXTERNALS_INSTALL_LOCATION}..."
  # Remove any previous installation before extracting
  rm -rf "${EXTERNALS_INSTALL_LOCATION}/go"
  tar -xzf "${tarball}" -C "${EXTERNALS_INSTALL_LOCATION}" >&2
  rm -f "${tarball}"
}

# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------
case "${OS}" in
  Linux)
    install_go_binary "linux"  "$(go_arch "${ARCH}")"
    ;;
  Darwin)
    install_go_binary "darwin" "$(go_arch "${ARCH}")"
    ;;
  *)
    die "Unsupported OS '${OS}' — install Go manually: https://go.dev/doc/install"
    ;;
esac

# ---------------------------------------------------------------------------
# Verify
# ---------------------------------------------------------------------------
[ -x "${GO_BIN}" ] \
  || die "Go installation failed: ${GO_BIN} not found after install"

INSTALLED_VER="$(go_installed_version "${GO_BIN}")"
log "go ${INSTALLED_VER} installed successfully"
echo "CVMFS_GO_DIR=${EXTERNALS_INSTALL_LOCATION}/go/bin"
