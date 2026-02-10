#!/bin/bash
#
# validate_unpacker.sh - Validate the CVMFS container unpacker (cvmfs_ducc)
#
# Compares the flat image produced by cvmfs_ducc convert-single-image with the
# filesystem obtained via docker export of the same image.
#
# Usage: validate_unpacker.sh [options] <image_url> [cvmfs_repo]
#
#   image_url   URL of the container image
#               e.g., https://registry.hub.docker.com/library/alpine:latest
#   cvmfs_repo  CVMFS repository name (default: test-validate.cern.ch)
#
# Options:
#   -c  Compare file contents (sha256 checksums), not just tree structure
#   -k  Keep temporary files after completion
#   -v  Verbose: print diffs to stdout
#   -h  Show this help message

set -euo pipefail

SCRIPT_NAME=$(basename "$0")
WORK_DIR=""
DOCKER_CONTAINER_ID=""
KEEP_TEMP=0
VERBOSE=0
COMPARE_CONTENT=0

# ── helpers ──────────────────────────────────────────────────────────────────

die()      { echo "[ERROR] $*" >&2; exit 1; }
log_info() { echo "[INFO]  $*"; }
log_dbg()  { [ "$VERBOSE" -eq 1 ] && echo "[DEBUG] $*" || true; }

usage() {
    sed -n '2,/^$/{ s/^# \?//; p }' "$0"
    exit 1
}

cleanup() {
    local rc=$?
    [ -n "$DOCKER_CONTAINER_ID" ] && docker rm "$DOCKER_CONTAINER_ID" >/dev/null 2>&1 || true
    if [ "$KEEP_TEMP" -eq 0 ] && [ -n "$WORK_DIR" ] && [ -d "$WORK_DIR" ]; then
        rm -rf "$WORK_DIR"
    fi
    exit $rc
}

# https://registry.hub.docker.com/library/alpine:latest  →  alpine:latest
url_to_docker_ref() {
    local ref="${1#*://}"
    # Docker Hub library images can be pulled with the short name
    ref="${ref#registry.hub.docker.com/library/}"
    echo "$ref"
}

# https://registry.hub.docker.com/library/alpine:latest
#   →  registry.hub.docker.com/library/alpine:latest
url_to_cvmfs_image_path() { echo "${1#*://}"; }

# Produce a deterministic, sorted listing of all entries in a directory.
# Format: TYPE PERMS SIZE PATH [-> SYMLINK_TARGET]
# Filters out CVMFS bookkeeping files and files that get special handling
# by the unpacker's singularity support (see ducc/singularity/dotfiles.go
# and ducc/singularity/startup_files.go):
#   - .singularity.d/ tree (created/overwritten by makeBaseEnv)
#   - /etc/hosts and /etc/resolv.conf (truncated by makeFiles)
#   - symlinks: singularity .run .exec .test .shell environment
generate_listing() {
    local dir="$1" output="$2"
    ( cd "$dir"
      find . \( -name '.cvmfscatalog' -o -name '.cvmfsdirtab' \
                -o -name '.cvmfsautocatalog' \
                -o -path './.singularity.d' \) -prune -o -print \
        | grep -v -x \
            -e '\./\.singularity\.d' \
            -e '\./etc/hosts' \
            -e '\./etc/resolv\.conf' \
            -e '\./singularity' \
            -e '\./\.run' \
            -e '\./\.exec' \
            -e '\./\.test' \
            -e '\./\.shell' \
            -e '\./environment' \
        | sort | while IFS= read -r entry; do
            if   [ -L "$entry" ]; then
                printf 'L %s -> %s\n' "$entry" "$(readlink "$entry")"
            elif [ -d "$entry" ]; then
                printf 'D %s\n' "$entry"
            elif [ -f "$entry" ]; then
                printf 'F %s %s\n' "$(stat -c '%s' "$entry")" "$entry"
            else
                printf '? %s\n' "$entry"
            fi
        done
    ) > "$output"
}

# sha256 checksums of every regular file (excluding CVMFS metadata and
# singularity special files — see generate_listing for details).
generate_checksums() {
    local dir="$1" output="$2"
    ( cd "$dir"
      find . -path './.singularity.d' -prune -o -type f \
             ! -name '.cvmfscatalog' \
             ! -name '.cvmfsdirtab' \
             ! -name '.cvmfsautocatalog' \
             ! -path './etc/hosts' \
             ! -path './etc/resolv.conf' \
             -print \
        | sort | xargs -r sha256sum
    ) > "$output"
}

# ── argument parsing ─────────────────────────────────────────────────────────

while getopts "ckvh" opt; do
    case $opt in
        c) COMPARE_CONTENT=1 ;;
        k) KEEP_TEMP=1 ;;
        v) VERBOSE=1 ;;
        h) usage ;;
        *) usage ;;
    esac
done
shift $((OPTIND - 1))

IMAGE_URL="${1:-}"
CVMFS_REPO="${2:-test-validate.cern.ch}"
[ -z "$IMAGE_URL" ] && { die "Missing required argument: image_url"; }

command -v cvmfs_ducc >/dev/null 2>&1 || die "cvmfs_ducc not found in PATH"
command -v docker     >/dev/null 2>&1 || die "docker not found in PATH"

trap cleanup EXIT HUP INT TERM

WORK_DIR=$(mktemp -d /tmp/validate_unpacker.XXXXXX)
DOCKER_REF=$(url_to_docker_ref "$IMAGE_URL")
CVMFS_IMAGE_PATH=$(url_to_cvmfs_image_path "$IMAGE_URL")

log_info "Image URL       : $IMAGE_URL"
log_info "Docker reference: $DOCKER_REF"
log_info "CVMFS repo      : $CVMFS_REPO"
log_info "Work dir        : $WORK_DIR"

# ── Step 1: unpack with cvmfs_ducc ──────────────────────────────────────────

log_info "=== Step 1: Unpacking with cvmfs_ducc ==="

# Create the CVMFS repo if it does not already exist
if ! cvmfs_server list 2>/dev/null | grep -q "^${CVMFS_REPO} "; then
    log_info "Creating CVMFS repository $CVMFS_REPO"
    sudo cvmfs_server mkfs -o "$USER" "$CVMFS_REPO" \
        || die "Failed to create CVMFS repository"
fi

log_info "Running: cvmfs_ducc convert-single-image -i -pls $IMAGE_URL $CVMFS_REPO"
cvmfs_ducc convert-single-image -i -p "$IMAGE_URL" "$CVMFS_REPO" \
    2>&1 | tee "$WORK_DIR/ducc.log" \
    || die "cvmfs_ducc conversion failed (see $WORK_DIR/ducc.log)"

CVMFS_FLAT_DIR="/cvmfs/$CVMFS_REPO/$CVMFS_IMAGE_PATH"
[ -d "$CVMFS_FLAT_DIR" ] || [ -L "$CVMFS_FLAT_DIR" ] \
    || die "Expected flat image not found at $CVMFS_FLAT_DIR"

# Resolve a potential symlink (.flat/xx/digest → public path)
CVMFS_FLAT_DIR=$(readlink -f "$CVMFS_FLAT_DIR")
log_info "Flat image resolved to: $CVMFS_FLAT_DIR"

# ── Step 2: export the same image from Docker ───────────────────────────────

log_info "=== Step 2: Exporting from Docker ==="

docker pull "$DOCKER_REF" || die "docker pull failed for $DOCKER_REF"

DOCKER_CONTAINER_ID=$(docker create "$DOCKER_REF" /bin/true)
log_info "Created container $DOCKER_CONTAINER_ID"

DOCKER_ROOTFS="$WORK_DIR/docker_rootfs"
mkdir -p "$DOCKER_ROOTFS"
docker export "$DOCKER_CONTAINER_ID" | tar -C "$DOCKER_ROOTFS" -xf -
log_info "Docker rootfs exported to $DOCKER_ROOTFS"

# ── Step 3: compare ─────────────────────────────────────────────────────────

log_info "=== Step 3: Comparing filesystems ==="

DUCC_LIST="$WORK_DIR/ducc_listing.txt"
DOCKER_LIST="$WORK_DIR/docker_listing.txt"

generate_listing "$CVMFS_FLAT_DIR" "$DUCC_LIST"
generate_listing "$DOCKER_ROOTFS"  "$DOCKER_LIST"

DUCC_N=$(wc -l < "$DUCC_LIST")
DOCKER_N=$(wc -l < "$DOCKER_LIST")
log_info "CVMFS flat image entries : $DUCC_N"
log_info "Docker export entries    : $DOCKER_N"

HAS_DIFF=0
LISTING_DIFF="$WORK_DIR/listing_diff.txt"

if diff -u "$DOCKER_LIST" "$DUCC_LIST" > "$LISTING_DIFF" 2>&1; then
    log_info "Tree structure comparison : PASS"
else
    HAS_DIFF=1
    ONLY_DOCKER=$(grep -c '^-[^-]' "$LISTING_DIFF" || true)
    ONLY_DUCC=$(grep -c '^+[^+]' "$LISTING_DIFF" || true)
    log_info "Tree structure comparison : FAIL"
    log_info "  Only in Docker export   : $ONLY_DOCKER entries"
    log_info "  Only in CVMFS flat image: $ONLY_DUCC entries"
    [ "$VERBOSE" -eq 1 ] && head -80 "$LISTING_DIFF"
fi

if [ "$COMPARE_CONTENT" -eq 1 ]; then
    log_info "Computing file checksums (this may take a while)..."
    DUCC_SUMS="$WORK_DIR/ducc_checksums.txt"
    DOCKER_SUMS="$WORK_DIR/docker_checksums.txt"
    SUMS_DIFF="$WORK_DIR/checksum_diff.txt"

    generate_checksums "$CVMFS_FLAT_DIR" "$DUCC_SUMS"
    generate_checksums "$DOCKER_ROOTFS"  "$DOCKER_SUMS"

    if diff -u "$DOCKER_SUMS" "$DUCC_SUMS" > "$SUMS_DIFF" 2>&1; then
        log_info "Content comparison        : PASS"
    else
        HAS_DIFF=1
        log_info "Content comparison        : FAIL"
        [ "$VERBOSE" -eq 1 ] && head -80 "$SUMS_DIFF"
    fi
fi

# ── Summary ──────────────────────────────────────────────────────────────────

echo ""
if [ "$HAS_DIFF" -eq 0 ]; then
    log_info "RESULT: PASS — flat image matches Docker export"
    log_info "  Image:   $IMAGE_URL"
    log_info "  Entries: $DOCKER_N"
    exit 0
else
    log_info "RESULT: FAIL — differences detected"
    log_info "  Image:   $IMAGE_URL"
    log_info "  Details: $WORK_DIR/"
    [ "$KEEP_TEMP" -eq 0 ] && log_info "  (re-run with -k to keep temp files)"
    exit 1
fi
