#!/usr/bin/env bash

set -euo pipefail

# Build/install or list build and runtime dependencies for CVMFS across Linux distros.
# Usage:
#   ci/build_install_builddeps.sh [OPTIONS] [<repo_root>]
# Options:
#   -l, --list       List build and runtime dependencies instead of installing
#   -i, --install    Install build dependencies (default)
#   --test-deps      Install dependencies needed by integration tests
#   -h, --help       Show this help
# Examples:
#   ci/build_install_builddeps.sh --list
#   ci/build_install_builddeps.sh --test-deps

########################
# Utilities
########################

die() { echo "ERROR: $*" >&2; exit 1; }
log() { echo "[builddeps] $*"; }
check_available() { command -v "$1" >/dev/null 2>&1; }

# cmake_version_ge A B — succeeds when cmake version A >= B
cmake_version_ge() {
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
  return 0
}

bootstrap_cmake_if_needed() {
  local min_ver="3.24.0"
  local current_ver=""
  if check_available cmake; then
    current_ver="$(cmake --version 2>/dev/null \
      | head -1 | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' || true)"
  fi
  if [ -n "$current_ver" ] && cmake_version_ge "$current_ver" "$min_ver"; then
    log "cmake ${current_ver} is sufficient (>= ${min_ver})"
    return 0
  fi
  if [ -n "$current_ver" ]; then
    log "cmake ${current_ver} < ${min_ver} — bootstrapping a newer cmake"
  else
    log "cmake not found — bootstrapping cmake"
  fi
  local cmake_dir_line
  cmake_dir_line="$("${SCRIPT_DIR}/bootstrap_cmake.sh")"
  eval "$cmake_dir_line"
  export CVMFS_CMAKE_DIR
  log "cmake bootstrapped at: ${CVMFS_CMAKE_DIR}/cmake"
}

bootstrap_golang_if_needed() {
  local min_ver="1.24.0"
  local current_ver=""
  if check_available go; then
    current_ver="$(go version 2>/dev/null \
      | grep -oE 'go[0-9]+\.[0-9]+(\.[0-9]+)?' | head -1 | sed 's/^go//' || true)"
  fi
  if [ -n "$current_ver" ] && cmake_version_ge "$current_ver" "$min_ver"; then
    log "go ${current_ver} is sufficient (>= ${min_ver})"
    return 0
  fi
  if [ -n "$current_ver" ]; then
    log "go ${current_ver} < ${min_ver} — bootstrapping a newer go"
  else
    log "go not found — bootstrapping go"
  fi
  local go_dir_line
  go_dir_line="$("${SCRIPT_DIR}/bootstrap_golang.sh")"
  eval "$go_dir_line"
  export CVMFS_GO_DIR
  log "go bootstrapped at: ${CVMFS_GO_DIR}/go"
}

get_script_dir() { cd "$(dirname "$0")" && pwd; }

########################
# Argument parsing
########################
MODE="install"
REPO_ARG=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    -l|--list) MODE="list"; shift;;
    -i|--install) MODE="install"; shift;;
    --test-deps) MODE="test-deps"; shift;;
    -h|--help)
      sed -n '5,15p' "$0" | sed 's/^# \{0,1\}//'; exit 0;;
    --) shift; break;;
    -*) die "Unknown option: $1";;
    *) REPO_ARG="$1"; shift;;
  esac
done

SCRIPT_DIR="$(get_script_dir)"
REPO_ROOT="${REPO_ARG:-$(cd "${SCRIPT_DIR}/.." && pwd)}"
[ -d "$REPO_ROOT" ] || die "Repository root '$REPO_ROOT' not found"

DEB_CONTROL="$REPO_ROOT/packaging/debian/cvmfs/control"
RPM_SPEC="$REPO_ROOT/packaging/rpm/cvmfs-universal.spec"

########################
# Privilege handling
########################
SUDO=""
if check_available sudo; then
  SUDO="sudo"
else
  if [[ "${MODE}" != "list" ]] && [[ ${EUID:-$(id -u)} -ne 0 ]]; then
    die "sudo not found and not running as root; cannot install packages"
  fi
fi

########################
# Platform detection
########################
PKG_MGR=""
OS_FAMILY=""   # deb|rhel|suse

detect_platform() {
  local id="" id_like=""
  if [[ -f /etc/os-release ]]; then
    # shellcheck disable=SC1091
    . /etc/os-release || true
    id="${ID:-}"
    id_like="${ID_LIKE:-}"
  fi
  local like_all="$id $id_like"
  if echo "$like_all" | grep -Eq '(debian|ubuntu)'; then
    PKG_MGR="apt-get"; OS_FAMILY="deb"; return 0
  fi
  if echo "$like_all" | grep -Eq '(rhel|centos|fedora|rocky|almalinux|ol)'; then
    PKG_MGR="dnf"; OS_FAMILY="rhel"; return 0
  fi
  if echo "$like_all" | grep -Eq '(suse|sles|opensuse)'; then
    PKG_MGR="zypper"; OS_FAMILY="suse"; return 0
  fi
  # Fallbacks by package manager availability
  if check_available apt-get; then PKG_MGR="apt-get"; OS_FAMILY="deb"; return 0; fi
  if check_available dnf; then PKG_MGR="dnf"; OS_FAMILY="rhel"; return 0; fi
  if check_available zypper; then PKG_MGR="zypper"; OS_FAMILY="suse"; return 0; fi
  return 1
}

detect_platform || die "Unsupported platform: cannot detect package manager"
log "Detected platform: $OS_FAMILY via $PKG_MGR"

########################
# Dependency extraction
########################
extract_deps_from_stanza() {
  # Common function to extract and clean dependency lists
  local filter_cvmfs="${1:-false}"
  sed -e 's/#.*$//' \
  | tr '\n' ' ' \
  | tr ',' '\n' \
  | sed -e 's/^\s*//; s/\s*$//' \
  | sed -E 's/\([^)]*\)//g' \
  | awk -F'|' '{gsub(/^\s+|\s+$/, "", $1); print $1}' \
  | sed -E 's/:[a-z0-9]+$//' \
  | grep -v '^\${' \
  | if [[ "$filter_cvmfs" = "true" ]]; then grep -v '^cvmfs'; else cat; fi \
  | sed -e '/^$/d'
}

list_deps_deb() {
  local control="$1"
  [ -f "$control" ] || die "Debian control file not found at $control"

  echo "# Build Dependencies:"
  # Extract Build-Depends
  awk 'BEGIN{inbd=0}
       /^Build-Depends:/ {inbd=1; sub(/^Build-Depends:/,""); print; next}
       inbd { if ($0 ~ /^[A-Za-z][-A-Za-z0-9]*:/) exit; print }' "$control" \
  | extract_deps_from_stanza false | sort -u

  echo ""
  echo "# Runtime Dependencies:"
  # Extract Depends from all Package sections
  awk '/^Package:/ {pkg=1; next}
       pkg && /^Depends:/ {dep=1; sub(/^Depends:/,""); print; next}
       pkg && dep && /^[A-Za-z][-A-Za-z0-9]*:/ {dep=0}
       pkg && dep {print}
       /^$/ {pkg=0; dep=0}' "$control" \
  | extract_deps_from_stanza true | sort -u
}

# extract_rpm_deps <spec> <rpmspec-query-flag> <tag>
# Requires rpmspec (part of the 'rpm-build' package) to correctly expand
# macros/conditionals. Parsing the raw spec text without it would silently
# union every distro variant's requirements together (e.g. both gcc4 and
# gcc, both sysvinit and systemd) and leave macros like %{cvmfs_go} and
# %{version}-%{release} unresolved, so that is not offered as a fallback.
extract_rpm_deps() {
  local spec="$1" query_flag="$2" tag="$3" out=""
  check_available rpmspec || \
    die "rpmspec not found (part of the 'rpm-build' package) -- install it first, e.g. 'dnf install rpm-build' or 'zypper install rpm-build'"

  out="$(rpmspec -q "$query_flag" "$spec" 2>/dev/null || true)"
  if [ -z "$out" ]; then
    # Older rpm without -q --buildrequires/--requires support: --parse still
    # expands macros/conditionals correctly, just without the per-dependency
    # splitting that -q provides.
    out="$(rpmspec --parse "$spec" 2>/dev/null | grep -E "^${tag}:" || true)"
    out="$(echo "$out" | sed -E "s/^${tag}:[[:space:]]*//" | tr -s '[:space:]' '\n')"
  fi
  echo "$out" \
    | sed -E 's/[[:space:]]*(<=|>=|<|>|=).*$//' \
    | grep -v '^/' \
    | sed -e '/^$/d' | sort -u
}

list_deps_rpm() {
  local spec="$1"
  [ -f "$spec" ] || die "RPM spec file not found at $spec"

  echo "# Build Dependencies:"
  extract_rpm_deps "$spec" --buildrequires "BuildRequires"

  echo ""
  echo "# Runtime Dependencies:"
  extract_rpm_deps "$spec" --requires "Requires" | grep -v '^cvmfs'
}

########################
# Installers per platform
########################
install_deps_deb() {
  [ -f "$DEB_CONTROL" ] || die "Debian control file not found at $DEB_CONTROL"

  export DEBIAN_FRONTEND=noninteractive
  $SUDO apt-get -y update
  if ! check_available mk-build-deps || ! dpkg -s equivs >/dev/null 2>&1; then
    $SUDO apt-get -y install devscripts equivs
  fi
  # Use mk-build-deps to create and install the meta-package, then remove it
  $SUDO mk-build-deps -t "apt-get -o Debug::pkgProblemResolver=yes --no-install-recommends -y" -i -r "$DEB_CONTROL"
}

install_deps_rhel() {
  [ -f "$RPM_SPEC" ] || die "RPM spec file not found at $RPM_SPEC"
  if ! check_available rpmbuild; then
    $SUDO dnf -y install rpm-build
  fi
  $SUDO dnf -y install dnf-plugins-core || true
  $SUDO dnf builddep -y "$RPM_SPEC" && return 0
  # Fallback: parse spec and install packages directly
  local pkgs
  pkgs=$(list_deps_rpm "$RPM_SPEC" | grep -v '^[[:space:]]*#' | grep -v '^[[:space:]]*$' || true)
  [ -n "${pkgs:-}" ] || die "Could not determine RPM build dependencies"
  $SUDO dnf -y install $pkgs
}

install_deps_suse() {
  [ -f "$RPM_SPEC" ] || die "RPM spec file not found at $RPM_SPEC"
  if ! check_available rpmbuild; then
    $SUDO zypper -n install rpm-build
  fi
  local pkgs
  pkgs=$(list_deps_rpm "$RPM_SPEC" | grep -v '^[[:space:]]*#' | grep -v '^[[:space:]]*$' || true)
  [ -n "${pkgs:-}" ] || die "Could not determine RPM build dependencies"
  $SUDO zypper -n install $pkgs
}

install_test_deps() {
  log "Installing integration test dependencies"
  case "$OS_FAMILY" in
    deb)
      export DEBIAN_FRONTEND=noninteractive
      $SUDO apt-get -y update
      $SUDO apt-get -y install g++
      ;;
    rhel) $SUDO dnf -y install gcc-c++;;
    suse) $SUDO zypper -n install gcc-c++;;
    *) die "Unsupported OS family for test dependencies: $OS_FAMILY";;
  esac
}

########################
# Main
########################
case "$MODE" in
  list)
    if [[ "$OS_FAMILY" = "deb" ]]; then
      list_deps_deb "$DEB_CONTROL"
    elif [[ "$OS_FAMILY" = "rhel" ]]; then
      list_deps_rpm "$RPM_SPEC"
    elif [[ "$OS_FAMILY" = "suse" ]]; then
      list_deps_rpm "$RPM_SPEC"
    else
      die "Unsupported OS family for listing: $OS_FAMILY"
    fi
    ;;
  install)
    if [[ "$OS_FAMILY" = "deb" ]]; then
      install_deps_deb
    elif [[ "$OS_FAMILY" = "rhel" ]]; then
      install_deps_rhel
    elif [[ "$OS_FAMILY" = "suse" ]]; then
      install_deps_suse
    else
      die "Unsupported OS family for install: $OS_FAMILY"
    fi
    log "Build dependencies installed successfully"
    bootstrap_cmake_if_needed
    bootstrap_golang_if_needed
    ;;
  test-deps)
    install_test_deps
    log "Integration test dependencies installed successfully"
    ;;
  *) die "Unknown mode: $MODE";;
esac
