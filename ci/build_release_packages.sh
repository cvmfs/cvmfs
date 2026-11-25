#!/usr/bin/env bash
#
# Build release packages for a git tag on several platforms via
# ci/build_package_remote.sh --detach. Remote builds keep running when this
# machine sleeps; re-run `status` / `fetch` afterwards.
#
# Usage:
#   ci/build_release_packages.sh start  <tag>   # export tag, launch all builds
#   ci/build_release_packages.sh status <tag>   # print per-platform status
#   ci/build_release_packages.sh fetch  <tag>   # copy finished packages back
#   ci/build_release_packages.sh macos  <tag>   # build the macOS pkg locally
#
# Environment:
#   X86_HOST (cvm-perf13), ARM_HOST (sftnight@macphsft38),
#   X86_SLOTS (6), ARM_SLOTS (2), PLATFORMS (all), OUT (./release-packages/<tag>)
#   APPLY_COMMITS: commits applied on top of the exported tag (packaging fixes
#   not yet in the tag)
#

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")"; pwd)
REPO=$(cd "$SCRIPT_DIR/.."; pwd)
REMOTE_BUILD="$SCRIPT_DIR/build_package_remote.sh"

X86_HOST=${X86_HOST:-cvm-perf13}
ARM_HOST=${ARM_HOST:-sftnight@macphsft38}
X86_SLOTS=${X86_SLOTS:-6}
ARM_SLOTS=${ARM_SLOTS:-2}

# One entry per platform. The image is derived from the name; append
# =<image> to override it. Images without a registry come from docker.io.
PLATFORMS=${PLATFORMS:-"
  cc8 cc9 cc10
  debian11 debian12 debian13
  fedora44 fedora45
  sles15
  ubuntu2204 ubuntu2404 ubuntu2604 ubuntu2610
"}

name_of() { echo "${1%%=*}"; }

image_for() {
  local p img v
  p=$(name_of "$1")
  if [[ $1 == *=* ]]; then
    img=${1#*=}
  else
    case "$p" in
      cc*)     img=almalinux:${p#cc} ;;
      debian*) img=debian:${p#debian} ;;
      fedora*) img=fedora:${p#fedora} ;;
      # SLES images need a subscription; Leap shares its code base.
      sles*)   img=opensuse/leap:${p#sles} ;;
      ubuntu*) v=${p#ubuntu}; img=ubuntu:${v:0:2}.${v:2} ;;
      *) echo "unknown platform: $p" >&2; return 1 ;;
    esac
  fi
  # A registry host is recognized by a dot, colon or "localhost" in the
  # first path component, as docker does it.
  case "$img" in
    */*) local first=${img%%/*}
         [[ $first == *.* || $first == *:* || $first == localhost ]] \
           || img=docker.io/$img ;;
    *)   img=docker.io/library/$img ;;
  esac
  echo "$img"
}

ARCHES="x86_64 aarch64"

[ $# -eq 2 ] || { sed -n '7,17p' "$0" | sed 's/^# \{0,1\}//'; exit 1; }
CMD=$1
TAG=$2
OUT=${OUT:-$REPO/release-packages/$TAG}
EXPORT="$OUT/src"

host_for()  { [ "$1" = x86_64 ] && echo "$X86_HOST" || echo "$ARM_HOST"; }
slots_for() { [ "$1" = x86_64 ] && echo "$X86_SLOTS" || echo "$ARM_SLOTS"; }
rdir_for()  { echo "/tmp/cvmfs-release-$TAG-$1-$2"; }

# Each launch opens ~15 ssh sessions; share one connection per host so jump
# hosts don't start resetting connections.
SSH_OPTS="-o ControlMaster=auto -o ControlPath=$HOME/.ssh/cm-release-%C -o ControlPersist=10m"

# git archive has no .git, so pin the version the tag resolves to.
export_tag() {
  git -C "$REPO" rev-parse -q --verify "refs/tags/$TAG" >/dev/null \
    || { echo "no such tag: $TAG" >&2; exit 1; }
  rm -rf "$EXPORT"
  mkdir -p "$EXPORT"
  git -C "$REPO" archive "$TAG" | tar -x -C "$EXPORT"
  local v=${TAG#cvmfs-}
  v=${v/-pre/\~pre}
  echo "$v" > "$EXPORT/CVMFS_VERSION"
  local c
  for c in ${APPLY_COMMITS:-}; do
    git -C "$REPO" show --format= "$c" | patch -p1 -d "$EXPORT" \
      || { echo "applying $c failed" >&2; exit 1; }
    echo "++ applied $(git -C "$REPO" log -1 --format='%h %s' "$c")"
  done
  echo "++ exported $TAG to $EXPORT (version $v)"
}

case "$CMD" in
  start)
    export_tag
    for arch in $ARCHES; do
      for e in $PLATFORMS; do
        p=$(name_of "$e")
        echo "=== launch $p/$arch"
        "$REMOTE_BUILD" --ssh-opts "$SSH_OPTS" --detach --clean --slots "$(slots_for "$arch")" \
          -s "$EXPORT" -i "$(image_for "$e")" -a "$arch" \
          -r "$(rdir_for "$p" "$arch")" -d "$OUT/$p/$arch" \
          "$(host_for "$arch")" > "$OUT/launch-$p-$arch.log" 2>&1 \
          || { echo "launch failed, see $OUT/launch-$p-$arch.log"; }
      done
    done
    ;;
  status)
    for arch in $ARCHES; do
      host=$(host_for "$arch")
      for e in $PLATFORMS; do
        p=$(name_of "$e")
        st=$(ssh $SSH_OPTS "$host" "cat '$(rdir_for "$p" "$arch")/status' 2>/dev/null || echo missing")
        printf '%-12s %-8s %s\n' "$p" "$arch" "$st"
      done
    done
    ;;
  fetch)
    for arch in $ARCHES; do
      for e in $PLATFORMS; do
        p=$(name_of "$e")
        "$REMOTE_BUILD" --ssh-opts "$SSH_OPTS" --fetch -r "$(rdir_for "$p" "$arch")" -d "$OUT/$p/$arch" \
          "$(host_for "$arch")" || true
      done
    done
    ;;
  macos)
    [ -d "$EXPORT" ] || export_tag
    rm -rf "$OUT/macos"
    mkdir -p "$OUT/macos"
    # Same environment as .github/workflows/ci_macos.yml.
    SDKROOT=$(xcrun --show-sdk-path) CVMFS_MACFUSE=ON CVMFS_EXTERNALS_EXCLUDE=zlib \
      CPATH="${CPATH:-}:/usr/local/include" \
      caffeinate -i "$EXPORT/ci/build_package.sh" "$EXPORT" "$OUT/macos" cvmfs
    ;;
  *) echo "unknown command: $CMD" >&2; exit 1 ;;
esac
