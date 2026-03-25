#!/bin/sh

set -eu

SCRIPT_NAME=$(basename "$0")
REPO_URL="https://github.com/cvmfs/cvmfs.git"
DRY_RUN=0
SKIP_DEPS=0
SKIP_BUILD=0
SKIP_TESTS=0
SOURCE_DIR=""
CLONE_DIR=""
BUILD_DIR=""
CURRENT_STEP="initialization"

usage() {
  cat <<EOF
Usage: $SCRIPT_NAME [OPTIONS] [SOURCE_TREE]

Bootstrap a CVMFS developer environment from an existing source tree or by
cloning CVMFS from GitHub.

Options:
  --clone-dir DIR   Clone into DIR when SOURCE_TREE is not provided
  --build-dir DIR   Use DIR as the build directory (default: SOURCE_TREE/build)
  --skip-deps       Skip ci/build_install_builddeps.sh
  --skip-build      Skip cmake configure/build
  --skip-tests      Skip targeted test execution
  --dry-run         Print the planned commands without executing them
  -h, --help        Show this help text

Examples:
  $SCRIPT_NAME .
  $SCRIPT_NAME --clone-dir /tmp/cvmfs-dev
  $SCRIPT_NAME --dry-run --skip-deps --skip-build --skip-tests .
EOF
}

log() {
  printf '%s\n' "[install-dev] $*"
}

die() {
  printf '%s\n' "[install-dev] ERROR: $*" >&2
  exit 1
}

set_step() {
  CURRENT_STEP="$1"
  log "==> $CURRENT_STEP"
}

run_cmd() {
  if [ "$DRY_RUN" -eq 1 ]; then
    log "DRY-RUN: $*"
    return 0
  fi

  log "RUN: $*"
  "$@" || die "Step '$CURRENT_STEP' failed while running: $*"
}

run_in_dir() {
  run_dir=$1
  shift

  if [ "$DRY_RUN" -eq 1 ]; then
    log "DRY-RUN: (cd $run_dir && $*)"
    return 0
  fi

  log "RUN: (cd $run_dir && $*)"
  (
    cd "$run_dir"
    "$@"
  ) || die "Step '$CURRENT_STEP' failed in '$run_dir' while running: $*"
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || die "Required command not found: $1"
}

resolve_existing_dir() {
  [ -d "$1" ] || die "Directory not found: $1"
  (
    cd "$1"
    pwd -P
  )
}

resolve_path() {
  case "$1" in
    /*) printf '%s\n' "$1" ;;
    *) printf '%s/%s\n' "$(pwd -P)" "$1" ;;
  esac
}

detect_jobs() {
  jobs=$(getconf _NPROCESSORS_ONLN 2>/dev/null || true)
  if [ -z "${jobs:-}" ] && command -v sysctl >/dev/null 2>&1; then
    jobs=$(sysctl -n hw.ncpu 2>/dev/null || true)
  fi
  if [ -z "${jobs:-}" ]; then
    jobs=1
  fi
  printf '%s\n' "$jobs"
}

ensure_repo_layout() {
  repo_root=$1
  [ -f "$repo_root/CMakeLists.txt" ] || die "Not a CVMFS source tree: $repo_root"
  [ -f "$repo_root/ci/build_install_builddeps.sh" ] || die "Missing dependency helper in $repo_root/ci"
}

while [ $# -gt 0 ]; do
  case "$1" in
    --clone-dir)
      shift
      [ $# -gt 0 ] || die "Missing value for --clone-dir"
      CLONE_DIR=$1
      ;;
    --build-dir)
      shift
      [ $# -gt 0 ] || die "Missing value for --build-dir"
      BUILD_DIR=$1
      ;;
    --skip-deps)
      SKIP_DEPS=1
      ;;
    --skip-build)
      SKIP_BUILD=1
      ;;
    --skip-tests)
      SKIP_TESTS=1
      ;;
    --dry-run)
      DRY_RUN=1
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    --)
      shift
      break
      ;;
    -*)
      die "Unknown option: $1"
      ;;
    *)
      if [ -n "$SOURCE_DIR" ]; then
        die "Only one SOURCE_TREE argument may be provided"
      fi
      SOURCE_DIR=$1
      ;;
  esac
  shift
done

if [ $# -gt 0 ]; then
  if [ -z "$SOURCE_DIR" ] && [ $# -eq 1 ]; then
    SOURCE_DIR=$1
    shift
  fi
fi

if [ $# -gt 0 ]; then
  die "Unexpected arguments: $*"
fi

if [ -n "$SOURCE_DIR" ] && [ -n "$CLONE_DIR" ]; then
  die "Use either SOURCE_TREE or --clone-dir, not both"
fi

if [ -n "$SOURCE_DIR" ]; then
  SOURCE_DIR=$(resolve_existing_dir "$SOURCE_DIR")
  log "Using existing source tree: $SOURCE_DIR"
else
  CLONE_DIR=${CLONE_DIR:-cvmfs-dev}
  SOURCE_DIR=$(resolve_path "$CLONE_DIR")
  if [ -e "$SOURCE_DIR" ]; then
    die "Clone destination already exists: $SOURCE_DIR"
  fi
  log "No source tree provided; will clone CVMFS into: $SOURCE_DIR"
fi

BUILD_DIR=${BUILD_DIR:-$SOURCE_DIR/build}
BUILD_DIR=$(resolve_path "$BUILD_DIR")

if [ -z "$SOURCE_DIR" ]; then
  die "Internal error: source directory resolution failed"
fi

if [ -n "${CLONE_DIR:-}" ] && [ ! -d "$SOURCE_DIR" ]; then
  set_step "Cloning CVMFS source tree"
  require_command git
  run_cmd git clone "$REPO_URL" "$SOURCE_DIR"
fi

if [ "$DRY_RUN" -eq 0 ]; then
  ensure_repo_layout "$SOURCE_DIR"
fi

if [ "$SKIP_DEPS" -eq 0 ]; then
  set_step "Installing build dependencies"
  require_command bash
  run_cmd bash "$SOURCE_DIR/ci/build_install_builddeps.sh" --install "$SOURCE_DIR"
else
  log "Skipping build dependency installation"
fi

if [ "$SKIP_BUILD" -eq 0 ]; then
  set_step "Configuring build directory"
  require_command cmake
  run_cmd mkdir -p "$BUILD_DIR"
  run_in_dir "$BUILD_DIR" cmake "$SOURCE_DIR" -DBUILD_ALL=ON -DBUILD_UNITTESTS=ON

  set_step "Building CVMFS"
  run_cmd cmake --build "$BUILD_DIR" --parallel "$(detect_jobs)"
else
  log "Skipping configure/build"
fi

if [ "$SKIP_TESTS" -eq 0 ]; then
  set_step "Running targeted unit tests"
  require_command ctest
  if [ "$DRY_RUN" -eq 0 ] && [ ! -d "$BUILD_DIR" ]; then
    die "Build directory not found for test execution: $BUILD_DIR"
  fi
  run_in_dir "$BUILD_DIR" ctest --output-on-failure -R cvmfs_unittests
else
  log "Skipping targeted tests"
fi

set_step "Bootstrap complete"
log "CVMFS developer bootstrap finished successfully"
