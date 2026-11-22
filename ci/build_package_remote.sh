#!/usr/bin/env sh

#
# Run a CernVM-FS package build on a remote ssh host (or locally) and copy
# the resulting packages back.
#
# Modes:
#   * native (default):     ssh to <host> and run ci/build_package.sh directly.
#                           The host must already have build deps installed.
#   * docker (--image):     build via BuildKit using
#                           ci/Dockerfile-package with BASE_IMAGE=IMAGE. Deps
#                           are installed inside the container by
#                           ci/build_install_builddeps.sh. Only
#                           the resulting packages are copied out.
#   * local (--local):      run docker build directly on this machine, no
#                           ssh/rsync. Recommended on macOS/Apple Silicon —
#                           use Colima (free OSS), Docker Desktop, or OrbStack
#                           as the docker provider; the script just calls the
#                           docker CLI on $PATH.
#
# Cross-architecture builds (e.g. aarch64 on an x86_64 host) require qemu's
# binfmt_misc handlers to be registered on the remote linux host — this
# script verifies the registration and refuses with install hints if missing.
# In --local mode, or when the remote host itself is macOS (Docker Desktop /
# Colima / OrbStack), the docker provider's own VM/Rosetta handles cross-arch
# and the binfmt_misc check is skipped.
#
# Requirements:
#   local mode:   docker CLI on PATH (Docker Desktop / Colima / OrbStack /
#                 podman). On Apple Silicon, native arm64 builds skip qemu.
#   remote modes: ssh + rsync locally; rsync on remote; for --image, docker
#                 with BuildKit support (>= 23, or DOCKER_BUILDKIT=1 on older)
#                 plus qemu-user-static + binfmt_misc when cross-building on
#                 a Linux remote. A macOS remote (Docker Desktop / Colima /
#                 OrbStack) works the same way; note that non-interactive ssh
#                 sessions on macOS don't source ~/.zprofile/~/.zshrc, so this
#                 script adds common Homebrew/Colima/OrbStack bin dirs to PATH
#                 itself when running remote commands.
#

set -e

usage() {
  cat <<EOF
Usage: $0 [options] <ssh-host|--local> [<package-name>] [-- <extra build args>]

Options:
  -i, --image IMAGE     Base image to build in (e.g. almalinux:9, debian:12).
                        Triggers a BuildKit build of test/common/container/
                        Dockerfile-dev with BASE_IMAGE=IMAGE. Outputs are
                        extracted into <local-dest>.
                        If omitted, the build runs natively on the remote host
                        and <package-name> selects which package to build.
  -L, --local           Run docker build on THIS machine — no ssh/rsync. The
                        positional <ssh-host> arg is not required (or pass "-").
                        Best path on macOS/Apple Silicon (use Colima, Docker
                        Desktop, or OrbStack as the docker provider).
  -a, --arch ARCH       Target architecture: x86_64, aarch64 or riscv64.
                        Default: the arch of the build host (the remote host,
                        or this machine with --local).
                        Requires qemu-user-static + binfmt_misc on the remote
                        when the target differs from the remote host arch.
                        In --local mode the docker provider handles cross-arch.
  -d, --dest DIR        Local directory to copy packages into.
                        Default: ./build-remote
  -r, --remote-dir DIR  Remote scratch directory (remote modes only).
                        Default: /tmp/cvmfs-build-<remote-user>
      --ssh-opts STR    Extra ssh options (e.g. "-J jump.example.com -p 2222").
      --clean           Wipe the remote scratch directory before syncing.
      --no-sync         Skip rsync; reuse what is already on the remote.
  -s, --source DIR      Source tree to build (default: the tree containing
                        this script). Use an exported tag tree for releases.
  -t, --tag TAG         Build git tag TAG (e.g. cvmfs-2.14.2) of the repo
                        containing this script, without checking it out. The
                        build helpers (Dockerfile-package, this script) still
                        come from the current tree.
      --detach          Start the remote build in the background (nohup) and
                        return; survives the local machine going away. Log:
                        <remote-dir>/build.log, result: <remote-dir>/status.
      --fetch           Only fetch packages of a previous --detach build.
                        Fails if that build is still running or failed.
      --slots N         With --detach: run at most N detached builds at once
                        on the remote host (queued via a lock directory).
  -h, --help            Show this help.

Notes:
  * In docker mode the Dockerfile builds the cvmfs packages;
    <package-name> and extra args are only used in native mode.

Examples:
  $0 cvm-perf13 cvmfs                              # native build on remote
  $0 -i almalinux:9 cvm-perf13                     # docker build on remote
  $0 -i almalinux:9 -a aarch64 cvm-perf13          # cross-arch via qemu
  $0 -L -i ubuntu:26.04 -a aarch64                 # local on Apple Silicon
  $0 -L -i almalinux:9 -a x86_64                   # local cross-arch via VM
  $0 -t cvmfs-2.14.2 -i almalinux:9 cvm-perf13     # build a release tag
EOF
}

IMAGE=""
ARCH=""
DEST="./build-remote"
REMOTE_DIR=""
SSH_OPTS=""
CLEAN=0
NO_SYNC=0
LOCAL=0
SOURCE_DIR=""
TAG=""
DETACH=0
FETCH=0
SLOTS=0

while [ $# -gt 0 ]; do
  case "$1" in
    -i|--image)      IMAGE="$2";      shift 2 ;;
    -a|--arch)       ARCH="$2";       shift 2 ;;
    -d|--dest)       DEST="$2";       shift 2 ;;
    -r|--remote-dir) REMOTE_DIR="$2"; shift 2 ;;
    --ssh-opts)      SSH_OPTS="$2";   shift 2 ;;
    --clean)         CLEAN=1;         shift ;;
    --no-sync)       NO_SYNC=1;       shift ;;
    -L|--local)      LOCAL=1;         shift ;;
    -s|--source)     SOURCE_DIR="$2"; shift 2 ;;
    -t|--tag)        TAG="$2";        shift 2 ;;
    --detach)        DETACH=1;        shift ;;
    --fetch)         FETCH=1;         shift ;;
    --slots)         SLOTS="$2";      shift 2 ;;
    -h|--help)       usage; exit 0 ;;
    --)              shift; break ;;
    -*)              echo "unknown option: $1" >&2; usage >&2; exit 1 ;;
    *)               break ;;
  esac
done

if [ "$LOCAL" = 0 ] && [ $# -lt 1 ]; then
  usage >&2
  exit 1
fi

if [ "$LOCAL" = 1 ]; then
  HOST=""
  # Allow but ignore a placeholder host arg like "-" or "local".
  if [ $# -gt 0 ]; then
    case "$1" in -|local|localhost) shift ;; esac
  fi
else
  HOST="$1"
  shift
fi
PACKAGE="${1:-cvmfs}"
[ $# -gt 0 ] && shift
EXTRA_ARGS="$@"

if [ "$LOCAL" = 1 ] && [ -z "$IMAGE" ]; then
  echo "error: --local requires --image (no docker-less local builds)" >&2
  exit 1
fi
if [ "$LOCAL" = 0 ] && [ -z "$IMAGE" ] && [ -z "$PACKAGE" ]; then
  echo "error: <package-name> is required for native builds" >&2
  exit 1
fi

set_arch() {
  case "$1" in
    x86_64|amd64)        ARCH=x86_64;  DOCKER_PLATFORM=linux/amd64 ;;
    aarch64|arm64)       ARCH=aarch64; DOCKER_PLATFORM=linux/arm64 ;;
    riscv64|riscv)       ARCH=riscv64; DOCKER_PLATFORM=linux/riscv64 ;;
    *) echo "unsupported --arch: $1 (expected x86_64, aarch64 or riscv64)" >&2; exit 1 ;;
  esac
}
# Remote mode defers the default until the remote arch is known.
if [ -n "$ARCH" ]; then
  set_arch "$ARCH"
elif [ "$LOCAL" = 1 ]; then
  set_arch "$(uname -m)"
fi

SCRIPT_DIR=$(cd "$(dirname "$0")"; pwd)
DOCKERFILE="$SCRIPT_DIR/Dockerfile-package"

if [ -n "$TAG" ] && [ "$FETCH" = 0 ]; then
  if [ -n "$SOURCE_DIR" ]; then
    echo "error: --tag and --source are mutually exclusive" >&2
    exit 1
  fi
  REPO_DIR=$(cd "$SCRIPT_DIR/.."; pwd)
  git -C "$REPO_DIR" rev-parse -q --verify "refs/tags/$TAG" >/dev/null \
    || { echo "error: no such tag: $TAG" >&2; exit 1; }
  SOURCE_DIR=$(mktemp -d "${TMPDIR:-/tmp}/$TAG.XXXXXX")
  trap 'rm -rf "$SOURCE_DIR"' EXIT
  git -C "$REPO_DIR" archive "$TAG" | tar -x -C "$SOURCE_DIR"
  # git archive has no .git for cmake/get_version.sh, so pin the version.
  echo "${TAG#cvmfs-}" | sed 's/-pre/~pre/' > "$SOURCE_DIR/CVMFS_VERSION"
  echo "++ exported tag $TAG (version $(cat "$SOURCE_DIR/CVMFS_VERSION"))"
fi
SOURCE_DIR=$(cd "${SOURCE_DIR:-$SCRIPT_DIR/..}"; pwd)

# ---------------------------------------------------------------------------
# Local mode: skip ssh/rsync/binfmt entirely. The local docker provider
# (Docker Desktop / Colima / OrbStack / podman) runs the build directly with
# $SOURCE_DIR as the build context, and BuildKit writes the artifacts stage
# into $DEST.
# ---------------------------------------------------------------------------
if [ "$LOCAL" = 1 ]; then
  echo "++ tag:            ${TAG:-<none>}"
  echo "++ local build (no ssh)"
  echo "++ source:         $SOURCE_DIR"
  echo "++ image:          $IMAGE"
  echo "++ target arch:    $ARCH"
  echo "++ local dest:     $DEST"

  if ! command -v docker >/dev/null 2>&1; then
    cat >&2 <<EOF
error: 'docker' not found in PATH. On macOS install one of:
  * Colima (free OSS):  brew install colima docker docker-buildx \\
                        && colima start --vm-type vz --arch aarch64
  * Docker Desktop:     https://www.docker.com/products/docker-desktop
  * OrbStack:           https://orbstack.dev
EOF
    exit 1
  fi

  mkdir -p "$DEST"
  cd "$SOURCE_DIR"
  echo "++ building via buildkit (BASE_IMAGE=$IMAGE, $DOCKER_PLATFORM)"
  echo "++ dockerfile: $DOCKERFILE (target: artifacts)"
  DOCKER_BUILDKIT=1 docker build \
      --platform "$DOCKER_PLATFORM" \
      --build-arg BASE_IMAGE="$IMAGE" \
      --target artifacts \
      --output "type=local,dest=$DEST" \
      -f "$DOCKERFILE" \
      .
  echo "++ done. packages in: $DEST"
  exit 0
fi

# shellcheck disable=SC2086
SSH="ssh $SSH_OPTS"
# shellcheck disable=SC2086
RSYNC_SSH="ssh $SSH_OPTS"

# Run a command on the remote host with a PATH that includes common Homebrew /
# Docker-provider install locations. Non-interactive ssh sessions on macOS only
# source ~/.zshenv (not ~/.zprofile or ~/.zshrc), so PATH additions made by the
# Homebrew/Colima/OrbStack installers (which live in .zprofile) are otherwise
# invisible to `ssh host cmd`, even though the same command works fine when
# run interactively.
rssh() {
  # shellcheck disable=SC2029
  $SSH "$HOST" "export PATH=\"\$HOME/.orbstack/bin:/opt/homebrew/bin:/usr/local/bin:\$PATH\"; $*"
}

REMOTE_INFO=$($SSH "$HOST" 'echo "$(uname -m):$USER:$HOME"')
REMOTE_ARCH_RAW="${REMOTE_INFO%%:*}"
REMOTE_INFO="${REMOTE_INFO#*:}"
REMOTE_USER="${REMOTE_INFO%%:*}"
REMOTE_HOME="${REMOTE_INFO#*:}"
# Native builds don't use ARCH, so any remote arch is fine there.
if [ -z "$ARCH" ]; then
  if [ -n "$IMAGE" ]; then set_arch "$REMOTE_ARCH_RAW"; else ARCH="$REMOTE_ARCH_RAW"; fi
fi
REMOTE_CACHE="$REMOTE_HOME/.cache"

if [ -z "$REMOTE_DIR" ]; then
  REMOTE_DIR="/tmp/cvmfs-build-${REMOTE_USER}"
fi
REMOTE_SRC="$REMOTE_DIR/src"
REMOTE_BUILD="$REMOTE_DIR/build"

if [ "$FETCH" = 1 ]; then
  STATUS=$(rssh "cat '$REMOTE_DIR/status' 2>/dev/null || echo missing")
  case "$STATUS" in
    ok) ;;
    *) echo "error: build in $HOST:$REMOTE_DIR not finished ok (status: $STATUS)" >&2
       exit 2 ;;
  esac
  mkdir -p "$DEST"
  rsync -az -e "$RSYNC_SSH" "$HOST:$REMOTE_BUILD/" "$DEST/"
  echo "++ fetched $HOST:$REMOTE_BUILD -> $DEST"
  exit 0
fi

echo "++ tag:            ${TAG:-<none>}"
echo "++ remote host:    $HOST"
echo "++ remote dir:     $REMOTE_DIR"
echo "++ remote cache:   $REMOTE_CACHE"
echo "++ source:         $SOURCE_DIR"
echo "++ package:        $PACKAGE"
echo "++ image:          ${IMAGE:-<native>}"
echo "++ target arch:    $ARCH"
echo "++ local dest:     $DEST"

# ---------------------------------------------------------------------------
# 1. prepare remote
# ---------------------------------------------------------------------------
if [ "$CLEAN" = 1 ]; then
  echo "++ cleaning $HOST:$REMOTE_DIR"
  rssh "rm -rf '$REMOTE_DIR'"
fi
rssh "mkdir -p '$REMOTE_SRC' '$REMOTE_BUILD' '$REMOTE_CACHE'"
# No glob: the remote login shell may be fish, which errors on no match.
rssh "rm -rf '$REMOTE_BUILD' '$REMOTE_DIR/status' && mkdir -p '$REMOTE_BUILD'"
# Buildkit writes outputs as root. Make the build dir world-writable so the
# rsync-back step works regardless of which user owns it.
rssh "chmod 777 '$REMOTE_BUILD' 2>/dev/null || true"

# ---------------------------------------------------------------------------
# 2. sync source
# ---------------------------------------------------------------------------
if [ "$NO_SYNC" = 0 ]; then
  echo "++ syncing source -> $HOST:$REMOTE_SRC"
  rsync -az --delete \
    --exclude='.git/' \
    --exclude='build*/' \
    --exclude='*.o' --exclude='*.pyc' \
    -e "$RSYNC_SSH" \
    "$SOURCE_DIR/" "$HOST:$REMOTE_SRC/"
fi
rsync -az -e "$RSYNC_SSH" "$DOCKERFILE" "$HOST:$REMOTE_DIR/Dockerfile-package"


# ---------------------------------------------------------------------------
# 3. arch sanity check (cross-arch needs binfmt_misc on a Linux remote; a
#    macOS remote's docker provider — Docker Desktop/Colima/OrbStack — runs
#    its own Linux VM and handles cross-arch emulation internally, the same
#    way --local mode does, so there is nothing to check there).
# ---------------------------------------------------------------------------
if [ -n "$IMAGE" ]; then
  REMOTE_KERNEL=$(rssh 'uname -s')
  case "$REMOTE_ARCH_RAW" in
    x86_64|amd64)   REMOTE_ARCH=x86_64 ;;
    aarch64|arm64)  REMOTE_ARCH=aarch64 ;;
    riscv64|riscv)  REMOTE_ARCH=riscv64 ;;
    *)              REMOTE_ARCH="$REMOTE_ARCH_RAW" ;;
  esac
  if [ "$REMOTE_KERNEL" != "Linux" ]; then
    echo "++ remote kernel is $REMOTE_KERNEL (not Linux) — skipping binfmt_misc check"
  elif [ "$REMOTE_ARCH" != "$ARCH" ]; then
    case "$ARCH" in
      aarch64) BINFMT_HANDLER=qemu-aarch64 ;;
      x86_64)  BINFMT_HANDLER=qemu-x86_64 ;;
      riscv64) BINFMT_HANDLER=qemu-riscv64 ;;
    esac
    if ! rssh "test -e /proc/sys/fs/binfmt_misc/$BINFMT_HANDLER"; then
      cat >&2 <<EOF
error: cross-arch build requested ($REMOTE_ARCH host -> $ARCH target) but
       /proc/sys/fs/binfmt_misc/$BINFMT_HANDLER is not registered on $HOST.

Install qemu-user-static on the remote, e.g.:
  rpm-based:  dnf install -y qemu-user-static
  deb-based:  apt install -y qemu-user-static binfmt-support
EOF
      exit 1
    fi
  fi
fi

# ---------------------------------------------------------------------------
# 4. run the build
# ---------------------------------------------------------------------------
if [ -n "$IMAGE" ]; then
  # If the remote's `docker` is actually podman with SELinux Enforcing, the
  # qemu interpreter registered via binfmt_misc gets blocked when invoked
  # inside the container. --security-opt label=disable bypasses labelling for
  # this container only and is a no-op on backends without SELinux.
  if ! rssh 'command -v docker >/dev/null 2>&1'; then
    cat >&2 <<EOF
error: 'docker' not found on $HOST (checked PATH plus \$HOME/.orbstack/bin,
       /opt/homebrew/bin, /usr/local/bin). Install a docker provider there,
       e.g. on macOS:
  * Colima (free OSS):  brew install colima docker docker-buildx \\
                        && colima start --vm-type vz
  * Docker Desktop:     https://www.docker.com/products/docker-desktop
  * OrbStack:           https://orbstack.dev
EOF
    exit 1
  fi

  # docker's CLI plugin search only covers $HOME/.docker/cli-plugins and a few
  # /usr/local paths. Homebrew on Apple Silicon installs docker-buildx under
  # /opt/homebrew, which isn't on that list, so `docker buildx` silently fails
  # to find an otherwise-present, correctly-installed plugin. Self-heal by
  # symlinking it into $HOME/.docker/cli-plugins if needed.
  if ! rssh 'docker buildx version >/dev/null 2>&1'; then
    BUILDX_BIN=$(rssh 'command -v docker-buildx' 2>/dev/null || true)
    if [ -n "$BUILDX_BIN" ]; then
      echo "++ docker buildx plugin not on docker's CLI plugin search path — linking $BUILDX_BIN into \$HOME/.docker/cli-plugins"
      rssh "mkdir -p \"\$HOME/.docker/cli-plugins\" && ln -sf '$BUILDX_BIN' \"\$HOME/.docker/cli-plugins/docker-buildx\""
    fi
    if ! rssh 'docker buildx version >/dev/null 2>&1'; then
      cat >&2 <<EOF
error: docker buildx plugin not available on $HOST (checked PATH and \$HOME/.docker/cli-plugins).
       Install it, e.g.:
  rpm-based:  dnf install -y docker-buildx-plugin
  deb-based:  apt install -y docker-buildx-plugin
  macOS:      brew install docker-buildx && mkdir -p ~/.docker/cli-plugins \\
              && ln -sf "\$(brew --prefix docker-buildx)/bin/docker-buildx" ~/.docker/cli-plugins/docker-buildx
EOF
      exit 1
    fi
  fi

  EXTRA_DOCKER_OPTS=""
  if rssh 'docker --version 2>&1 | grep -qi podman' \
      && rssh 'command -v getenforce >/dev/null && [ "$(getenforce)" = "Enforcing" ]'; then
    EXTRA_DOCKER_OPTS="--security-opt label=disable"
    echo "++ podman + SELinux Enforcing detected — adding $EXTRA_DOCKER_OPTS"
  fi

  echo "++ building via buildkit (BASE_IMAGE=$IMAGE, $DOCKER_PLATFORM) on $HOST"
  echo "++ dockerfile: ci/Dockerfile-package (target: artifacts)"
  BUILD_CMD="cd '$REMOTE_SRC' && DOCKER_BUILDKIT=1 docker build \
      $EXTRA_DOCKER_OPTS \
      --platform $DOCKER_PLATFORM \
      --build-arg BASE_IMAGE='$IMAGE' \
      --target artifacts \
      --output type=local,dest='$REMOTE_BUILD' \
      -f '$REMOTE_DIR/Dockerfile-package' \
      ."
else
  echo "++ building natively on $HOST"
  BUILD_CMD="cd '$REMOTE_BUILD' && \
      CVMFS_EXTERNALS_PREFIX='$REMOTE_CACHE' \
      '$REMOTE_SRC/ci/build_package.sh' \
        '$REMOTE_SRC' '$REMOTE_BUILD' '$PACKAGE' $EXTRA_ARGS"
fi

if [ "$DETACH" = 1 ]; then
  # mkdir is atomic everywhere (macOS has no flock), so slots are directories.
  QUEUE_DIR="/tmp/cvmfs-build-queue-${REMOTE_USER}"
  $SSH "$HOST" "cat > '$REMOTE_DIR/run.sh'" <<EOF
#!/bin/sh
export PATH="\$HOME/.orbstack/bin:/opt/homebrew/bin:/usr/local/bin:\$PATH"
echo queued > '$REMOTE_DIR/status'
slot=""
if [ "$SLOTS" -gt 0 ]; then
  mkdir -p '$QUEUE_DIR'
  while [ -z "\$slot" ]; do
    i=1
    while [ \$i -le $SLOTS ]; do
      if mkdir '$QUEUE_DIR/slot'\$i 2>/dev/null; then slot='$QUEUE_DIR/slot'\$i; break; fi
      i=\$((i + 1))
    done
    [ -n "\$slot" ] || sleep 20
  done
  trap 'rmdir "\$slot"' EXIT
fi
echo running > '$REMOTE_DIR/status'
date
if ( $BUILD_CMD ); then echo ok > '$REMOTE_DIR/status'
else echo "failed rc=\$?" > '$REMOTE_DIR/status'; fi
date
EOF
  rssh "nohup sh '$REMOTE_DIR/run.sh' > '$REMOTE_DIR/build.log' 2>&1 < /dev/null &"
  echo "++ detached. log: $HOST:$REMOTE_DIR/build.log"
  echo "++ fetch later: $0 --fetch -r '$REMOTE_DIR' -d '$DEST' $HOST"
  exit 0
fi

rssh "$BUILD_CMD"

# ---------------------------------------------------------------------------
# 5. fetch packages
# ---------------------------------------------------------------------------
mkdir -p "$DEST"
echo "++ fetching packages -> $DEST"
rsync -az -e "$RSYNC_SSH" "$HOST:$REMOTE_BUILD/" "$DEST/"

echo "++ done. packages in: $DEST"
