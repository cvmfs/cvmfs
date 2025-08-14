#!/usr/bin/env bash

set -euo pipefail

# Build/install or list build and runtime dependencies for CVMFS across Linux distros.
# Usage:
#   ci/build_install_builddeps.sh [OPTIONS] [<repo_root>]
# Options:
#   -l, --list       List build and runtime dependencies instead of installing
#   -i, --install    Install build dependencies (default)
#   -h, --help       Show this help
# Examples:
#   ci/build_install_builddeps.sh --list
#   ci/build_install_builddeps.sh --install /path/to/cvmfs

########################
# Utilities
########################

die() { echo "ERROR: $*" >&2; exit 1; }
log() { echo "[builddeps] $*"; }
check_available() { command -v "$1" >/dev/null 2>&1; }

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
    -h|--help)
      sed -n '1,20p' "$0" | sed 's/^# \{0,1\}//'; exit 0;;
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
  if [[ "${MODE}" = "install" ]] && [[ ${EUID:-$(id -u)} -ne 0 ]]; then
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
    if check_available dnf; then PKG_MGR="dnf"; else PKG_MGR="yum"; fi
    OS_FAMILY="rhel"; return 0
  fi
  if echo "$like_all" | grep -Eq '(suse|sles|opensuse)'; then
    PKG_MGR="zypper"; OS_FAMILY="suse"; return 0
  fi
  # Fallbacks by package manager availability
  if check_available apt-get; then PKG_MGR="apt-get"; OS_FAMILY="deb"; return 0; fi
  if check_available dnf; then PKG_MGR="dnf"; OS_FAMILY="rhel"; return 0; fi
  if check_available yum; then PKG_MGR="yum"; OS_FAMILY="rhel"; return 0; fi
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

list_deps_rpm() {
  local spec="$1"
  [ -f "$spec" ] || die "RPM spec file not found at $spec"

  echo "# Build Dependencies:"
  if check_available rpmspec; then
    rpmspec --parse "$spec" 2>/dev/null | grep -E '^BuildRequires:' || true
  else
    grep -E '^BuildRequires:' "$spec" || true
  fi | awk '{print $2}' | sed -e '/^$/d' | sort -u

  echo ""
  echo "# Runtime Dependencies:"
  if check_available rpmspec; then
    rpmspec --parse "$spec" 2>/dev/null | grep -E '^Requires:' || true
  else
    grep -E '^Requires:' "$spec" || true
  fi | awk '{print $2}' | sed -e '/^$/d' | grep -v '^cvmfs' | sort -u
}

########################
# Installers per platform
########################
install_deps_deb() {
  [ -f "$DEB_CONTROL" ] || die "Debian control file not found at $DEB_CONTROL"
  $SUDO apt-get -y update
  if ! check_available mk-build-deps; then
    $SUDO apt-get -y install devscripts equivs
  fi
  # Use mk-build-deps to create and install the meta-package, then remove it
  $SUDO DEBIAN_FRONTEND=noninteractive mk-build-deps -i -r "$DEB_CONTROL"
}

install_deps_rhel() {
  [ -f "$RPM_SPEC" ] || die "RPM spec file not found at $RPM_SPEC"
  if [[ "$PKG_MGR" = "dnf" ]]; then
    $SUDO dnf -y install dnf-plugins-core || true
    if check_available dnf; then
      $SUDO dnf builddep -y "$RPM_SPEC" && return 0
    fi
  fi
  if [[ "$PKG_MGR" = "yum" ]]; then
    $SUDO yum -y install yum-utils || true
    if check_available yum-builddep; then
      $SUDO yum-builddep -y "$RPM_SPEC" && return 0
    else
      $SUDO yum builddep -y "$RPM_SPEC" && return 0 || true
    fi
  fi
  # Fallback: parse spec and install packages directly
  local pkgs
  pkgs=$(list_deps_rpm "$RPM_SPEC" || true)
  [ -n "${pkgs:-}" ] || die "Could not determine RPM build dependencies"
  if [[ "$PKG_MGR" = "dnf" ]]; then
    $SUDO dnf -y install $pkgs
  else
    $SUDO yum -y install $pkgs
  fi
}

install_deps_suse() {
  [ -f "$RPM_SPEC" ] || die "RPM spec file not found at $RPM_SPEC"
  if ! check_available rpmbuild; then
    $SUDO zypper -n install rpm-build
  fi
  local pkgs
  pkgs=$(list_deps_rpm "$RPM_SPEC" || true)
  [ -n "${pkgs:-}" ] || die "Could not determine RPM build dependencies"
  $SUDO zypper -n install $pkgs
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
    ;;
  *) die "Unknown mode: $MODE";;
esac
