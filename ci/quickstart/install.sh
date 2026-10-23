#!/bin/sh

set -eu

DOC_URL="https://cvmfs.readthedocs.io/en/stable/cpt-quickstart.html"
APT_RELEASE_URL="https://cvmrepo.s3.cern.ch/cvmrepo/apt/cvmfs-release-latest_all.deb"
RPM_RELEASE_URL="https://cvmrepo.s3.cern.ch/cvmrepo/yum/cvmfs-release-latest.noarch.rpm"
SUSE_GPG_URL="https://cvmrepo.web.cern.ch/cvmrepo/yum/RPM-GPG-KEY-CernVM-2048"

DRY_RUN=0
SKIP_CHECKS=0
PLATFORM=""
PLATFORM_LABEL=""
KERNEL_NAME=""
ARCH_NAME=""
OS_LABEL=""
USE_SUDO=0
BREW_BIN="brew"
BREW_RUN_AS=""

usage() {
  cat <<EOF
Usage: sh install.sh [--dry-run]

Install the CVMFS client using the official quickstart package-install flow for
the detected platform.

Options:
  -n, --dry-run     Print the selected commands without executing them
  -h, --help        Show this help text

Validation overrides:
  CVMFS_INSTALL_UNAME_S          Override uname -s for dry-run validation
  CVMFS_INSTALL_UNAME_M          Override uname -m for dry-run validation
  CVMFS_INSTALL_OS_RELEASE_FILE  Override /etc/os-release for dry-run validation

Documentation: $DOC_URL
EOF
}

log() {
  printf '%s\n' "install.sh: $*"
}

warn() {
  printf '%s\n' "install.sh: warning: $*" >&2
}

die() {
  printf '%s\n' "install.sh: error: $*" >&2
  exit 1
}

have_cmd() {
  command -v "$1" >/dev/null 2>&1
}

to_lower() {
  printf '%s' "$1" | tr '[:upper:]' '[:lower:]'
}

detect_uname_s() {
  if [ -n "${CVMFS_INSTALL_UNAME_S:-}" ]; then
    printf '%s\n' "$CVMFS_INSTALL_UNAME_S"
  else
    uname -s
  fi
}

detect_uname_m() {
  if [ -n "${CVMFS_INSTALL_UNAME_M:-}" ]; then
    printf '%s\n' "$CVMFS_INSTALL_UNAME_M"
  else
    uname -m
  fi
}

normalize_arch() {
  case "$(to_lower "$1")" in
    x86_64|amd64) printf '%s\n' "amd64" ;;
    i386|i486|i586|i686|x86) printf '%s\n' "x86" ;;
    aarch64|arm64) printf '%s\n' "arm64" ;;
    armv6l|armv7|armv7l|armhf) printf '%s\n' "arm" ;;
    *) printf '%s\n' "$(to_lower "$1")" ;;
  esac
}

print_command() {
  printf '+ %s\n' "$*"
}

run_cmd() {
  print_command "$@"
  if [ "$DRY_RUN" -eq 1 ]; then
    return 0
  fi
  if "$@"; then
    return 0
  else
    status=$?
    die "Command failed with exit code $status: $*"
  fi
}

run_root_cmd() {
  if [ "$USE_SUDO" -eq 1 ]; then
    run_cmd sudo "$@"
  else
    run_cmd "$@"
  fi
}

run_brew_cmd() {
  if [ -n "$BREW_RUN_AS" ]; then
    run_cmd sudo -H -u "$BREW_RUN_AS" "$BREW_BIN" "$@"
  else
    run_cmd "$BREW_BIN" "$@"
  fi
}

require_cmd() {
  if [ "$SKIP_CHECKS" -eq 1 ]; then
    return 0
  fi
  if ! have_cmd "$1"; then
    die "Missing required command '$1' for $PLATFORM_LABEL. See $DOC_URL"
  fi
}

load_os_release() {
  OS_ID=""
  OS_ID_LIKE=""
  OS_LABEL="Linux"
  os_release_file="${CVMFS_INSTALL_OS_RELEASE_FILE:-/etc/os-release}"
  if [ -r "$os_release_file" ]; then
    ID=""
    ID_LIKE=""
    PRETTY_NAME=""
    NAME=""
    # shellcheck disable=SC1090
    . "$os_release_file" || die "Failed to read os-release data from $os_release_file"
    OS_ID="$(to_lower "${ID:-}")"
    OS_ID_LIKE="$(to_lower "${ID_LIKE:-}")"
    if [ -n "${PRETTY_NAME:-}" ]; then
      OS_LABEL="$PRETTY_NAME"
    elif [ -n "${NAME:-}" ]; then
      OS_LABEL="$NAME"
    fi
  fi
}

classify_linux_platform() {
  set -- $OS_ID $OS_ID_LIKE
  for token in "$@"; do
    case "$token" in
      fedora)
        printf '%s\n' "fedora"
        return 0
        ;;
      debian|ubuntu)
        printf '%s\n' "debian"
        return 0
        ;;
      suse|sles|opensuse|opensuse-leap|opensuse-tumbleweed)
        printf '%s\n' "suse"
        return 0
        ;;
      rhel|centos|rocky|almalinux|scientific|ol)
        printf '%s\n' "rhel"
        return 0
        ;;
    esac
  done
  return 1
}

detect_platform() {
  KERNEL_NAME="$(to_lower "$(detect_uname_s)")"
  ARCH_NAME="$(normalize_arch "$(detect_uname_m)")"

  case "$KERNEL_NAME" in
    darwin)
      case "$ARCH_NAME" in
        amd64|arm64) ;;
        *) die "Unsupported macOS architecture '$ARCH_NAME'. See $DOC_URL" ;;
      esac
      PLATFORM="macos"
      PLATFORM_LABEL="macOS/Homebrew"
      OS_LABEL="macOS"
      ;;
    linux)
      case "$ARCH_NAME" in
        amd64|x86|arm64|arm) ;;
        *) die "Unsupported Linux architecture '$ARCH_NAME'. See $DOC_URL" ;;
      esac
      load_os_release
      PLATFORM="$(classify_linux_platform || true)"
      case "$PLATFORM" in
        debian) PLATFORM_LABEL="Debian/Ubuntu" ;;
        rhel) PLATFORM_LABEL="RHEL-family" ;;
        fedora) PLATFORM_LABEL="Fedora" ;;
        suse) PLATFORM_LABEL="SUSE" ;;
        *)
          distro_hint="$OS_LABEL"
          if [ -n "$OS_ID" ]; then
            distro_hint="$distro_hint (ID=$OS_ID ID_LIKE=${OS_ID_LIKE:-n/a})"
          fi
          die "Unsupported Linux distribution: $distro_hint. Supported install paths are Debian/Ubuntu, RHEL-family, Fedora, SUSE, and macOS. See $DOC_URL"
          ;;
      esac
      ;;
    *)
      die "Unsupported operating system '$KERNEL_NAME'. Supported install paths are Debian/Ubuntu, RHEL-family, Fedora, SUSE, and macOS. See $DOC_URL"
      ;;
  esac
}

setup_linux_privileges() {
  if [ "$DRY_RUN" -eq 1 ]; then
    return 0
  fi
  if [ "$(id -u)" -eq 0 ]; then
    return 0
  fi
  if have_cmd sudo; then
    USE_SUDO=1
    return 0
  fi
  die "This install path needs root privileges. Re-run with sudo (for example: curl ... | sudo sh) or as root. See $DOC_URL"
}

find_brew() {
  if [ -n "${CVMFS_INSTALL_BREW_BIN:-}" ]; then
    BREW_BIN="$CVMFS_INSTALL_BREW_BIN"
    return 0
  fi
  if have_cmd brew; then
    BREW_BIN="$(command -v brew)"
    return 0
  fi
  for candidate in /opt/homebrew/bin/brew /usr/local/bin/brew; do
    if [ -x "$candidate" ]; then
      BREW_BIN="$candidate"
      return 0
    fi
  done
  return 1
}

setup_brew_context() {
  if [ "$SKIP_CHECKS" -eq 0 ] && ! find_brew; then
    die "Homebrew ('brew') is required for the macOS quickstart path. Install Homebrew first, then re-run this script. See $DOC_URL"
  fi
  if [ "$DRY_RUN" -eq 1 ] && [ ! -x "$BREW_BIN" ]; then
    BREW_BIN="brew"
  fi
  if [ "$(id -u)" -eq 0 ]; then
    if [ -n "${SUDO_USER:-}" ] && [ "$SUDO_USER" != "root" ]; then
      BREW_RUN_AS="$SUDO_USER"
      return 0
    fi
    if [ "$DRY_RUN" -eq 1 ]; then
      warn "Dry-run macOS validation as root cannot confirm the Homebrew user context; commands are printed only."
      return 0
    fi
    die "Homebrew must run as a non-root user. Re-run from an admin shell via sudo so SUDO_USER is set, or run the Homebrew quickstart commands manually as your regular user. See $DOC_URL"
  fi
}

install_debian() {
  require_cmd wget
  require_cmd dpkg
  require_cmd apt-get
  run_root_cmd wget "$APT_RELEASE_URL"
  run_root_cmd dpkg -i cvmfs-release-latest_all.deb
  run_root_cmd rm -f cvmfs-release-latest_all.deb
  run_root_cmd apt-get -y update
  run_root_cmd apt-get -y install cvmfs
}

install_rhel() {
  require_cmd yum
  run_root_cmd yum install -y "$RPM_RELEASE_URL"
  run_root_cmd yum install -y cvmfs
}

install_fedora() {
  require_cmd dnf
  run_root_cmd dnf install -y "$RPM_RELEASE_URL"
  run_root_cmd dnf install -y cvmfs
}

install_suse() {
  require_cmd rpm
  require_cmd zypper
  run_root_cmd rpm --import "$SUSE_GPG_URL"
  run_root_cmd zypper install -y "$RPM_RELEASE_URL"
  run_root_cmd zypper install -y cvmfs
}

install_macos() {
  setup_brew_context
  run_brew_cmd tap macos-fuse-t/cask
  run_brew_cmd tap cvmfs/homebrew-cvmfs
  run_brew_cmd install cvmfs
}

while [ $# -gt 0 ]; do
  case "$1" in
    -n|--dry-run|--print-only)
      DRY_RUN=1
      SKIP_CHECKS=1
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      die "Unknown option: $1"
      ;;
  esac
  shift
done

detect_platform

log "Detected system: os=$OS_LABEL kernel=$KERNEL_NAME arch=$ARCH_NAME"
log "Selected install path: $PLATFORM_LABEL"
if [ "$DRY_RUN" -eq 1 ]; then
  log "Dry-run mode enabled; commands will be printed but not executed."
fi

case "$PLATFORM" in
  debian|rhel|fedora|suse)
    setup_linux_privileges
    ;;
esac

case "$PLATFORM" in
  debian) install_debian ;;
  rhel) install_rhel ;;
  fedora) install_fedora ;;
  suse) install_suse ;;
  macos) install_macos ;;
  *) die "Internal error: unsupported platform '$PLATFORM'" ;;
esac

if [ "$DRY_RUN" -eq 1 ]; then
  log "Dry-run completed successfully."
else
  log "CVMFS install flow completed.

  Next steps:

  1. Add a suitable CVMFS_HTTP_PROXY to /etc/cvmfs/default.local, e.g.:
       CVMFS_HTTP_PROXY=DIRECT
     (Use DIRECT only for single workstations - for clusters, set up a dedicated proxy!)

  2. Run setup to enable autofs:
       cvmfs_config setup
       ls /cvmfs/cvmfs-config.cern.ch

     Or mount a repository directly:
       sudo mkdir -p /cvmfs/cvmfs-config.cern.ch
       sudo mount -t cvmfs cvmfs-config.cern.ch /cvmfs/cvmfs-config.cern.ch

  See $DOC_URL for more details."
fi
