#!/bin/bash
# redo-overlays.sh
#
# For a list of flat image paths in a CVMFS repository, finds the symlinks
# pointing to them and redoes the overlay conversion for each image.
#
# For each flat path it will:
#   1. Find all symlinks in the repo (outside .flat/) that point to the flat dir
#   2. cvmfs_server ingest --fast-delete <flat_path>
#   3. cvmfs_ducc delete-image <repo> <image_url>
#   4. cvmfs_ducc convert-single-image <image_url> <repo>

set -euo pipefail

# ---------------------------------------------------------------------------
# Defaults / constants
# ---------------------------------------------------------------------------
CVMFS_REPO_BASE="/cvmfs/unpacked.cern.ch"
CVMFS_REPO_NAME="unpacked.cern.ch"
# Scheme prepended to the symlink-derived path to build a full image URL
IMAGE_SCHEME="https://"

DRY_RUN=false
INPUT_FILE=""
CONVERT_EXTRA_ARGS="-p"   # -p = skip-podman; extend as needed

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
log()  { echo "[$(date '+%H:%M:%S')] $*"; }
warn() { echo "[$(date '+%H:%M:%S')] WARNING: $*" >&2; }
err()  { echo "[$(date '+%H:%M:%S')] ERROR: $*" >&2; }

run() {
    if $DRY_RUN; then
        echo "  [DRY-RUN] $*"
    else
        log "  Running: $*"
        "$@"
    fi
}

usage() {
    cat <<EOF
Usage: $0 [OPTIONS] [flat_path ...]

Finds all image symlinks pointing at the given flat paths, deletes the flat
overlay and reconverts each image.

OPTIONS:
  -f FILE    Read flat paths from FILE (one per line, blank lines / # ignored)
  -r REPO    CVMFS repository name          (default: $CVMFS_REPO_NAME)
  -b BASE    CVMFS repository mount base    (default: $CVMFS_REPO_BASE)
  -s SCHEME  URL scheme for image refs      (default: $IMAGE_SCHEME)
  -e ARGS    Extra args for convert-single-image (default: "$CONVERT_EXTRA_ARGS")
  -n         Dry run – print commands without executing
  -h         Show this help and exit

FLAT_PATH example:
  /cvmfs/unpacked.cern.ch/.flat/de/deb62cdf32f23e2390e288db4e90d201cd74aa054d5bc6ac0b08100b138b9503/

The image URL is derived from each symlink's path by stripping the repo base
and prepending SCHEME, e.g.:
  /cvmfs/unpacked.cern.ch/registry.hub.docker.com/library/ubuntu:22.04
  -> https://registry.hub.docker.com/library/ubuntu:22.04
EOF
    exit "${1:-0}"
}

# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------
while getopts ":f:r:b:s:e:nh" opt; do
    case $opt in
        f) INPUT_FILE="$OPTARG" ;;
        r) CVMFS_REPO_NAME="$OPTARG" ;;
        b) CVMFS_REPO_BASE="$OPTARG" ;;
        s) IMAGE_SCHEME="$OPTARG" ;;
        e) CONVERT_EXTRA_ARGS="$OPTARG" ;;
        n) DRY_RUN=true ;;
        h) usage 0 ;;
        :) err "Option -$OPTARG requires an argument."; usage 1 ;;
        \?) err "Unknown option: -$OPTARG"; usage 1 ;;
    esac
done
shift $((OPTIND - 1))

# Collect flat paths from positional args and/or input file
FLAT_PATHS=()
for arg in "$@"; do
    FLAT_PATHS+=("$arg")
done
if [[ -n "$INPUT_FILE" ]]; then
    if [[ ! -f "$INPUT_FILE" ]]; then
        err "Input file not found: $INPUT_FILE"
        exit 1
    fi
    while IFS= read -r line; do
        # Skip blank lines and comments
        [[ -z "$line" || "$line" == \#* ]] && continue
        FLAT_PATHS+=("$line")
    done < "$INPUT_FILE"
fi

if [[ ${#FLAT_PATHS[@]} -eq 0 ]]; then
    err "No flat paths provided."
    usage 1
fi

# ---------------------------------------------------------------------------
# Find symlinks pointing at a given flat directory
#
# Strategy: extract the hash from the flat path and use find -lname to match
# symlinks whose target contains that hash – avoids a full readlink loop over
# potentially millions of entries.
# ---------------------------------------------------------------------------
find_symlinks_for_flat() {
    local flat_path="$1"
    # Strip trailing slash for consistent handling
    flat_path="${flat_path%/}"

    # Extract the layer hash (last path component)
    local hash
    hash=$(basename "$flat_path")

    # Search for symlinks whose target contains the hash, excluding .flat/ itself
    find "$CVMFS_REPO_BASE" \
        -not -path "$CVMFS_REPO_BASE/.flat*" \
        -type l \
        -lname "*${hash}*" \
        2>/dev/null
}

# ---------------------------------------------------------------------------
# Derive a full image URL from a symlink path
# e.g. /cvmfs/unpacked.cern.ch/registry.hub.docker.com/library/ubuntu:22.04
#   -> https://registry.hub.docker.com/library/ubuntu:22.04
# ---------------------------------------------------------------------------
symlink_to_image_url() {
    local symlink="$1"
    local rel="${symlink#"${CVMFS_REPO_BASE}/"}"
    echo "${IMAGE_SCHEME}${rel}"
}

# ---------------------------------------------------------------------------
# Process one flat path
# ---------------------------------------------------------------------------
process_flat_path() {
    local flat_path="$1"
    flat_path="${flat_path%/}"   # normalise

    log "========================================"
    log "Flat path: $flat_path"

    # Validate that this path looks like a flat dir inside the repo
    if [[ "$flat_path" != "${CVMFS_REPO_BASE}/.flat/"* ]]; then
        warn "Path does not look like a .flat entry under $CVMFS_REPO_BASE – skipping."
        return 0
    fi

    # ------------------------------------------------------------------
    # 1. Find all symlinks pointing at this flat dir
    # ------------------------------------------------------------------
    log "Searching for symlinks pointing at $flat_path ..."
    local symlinks=()
    while IFS= read -r sym; do
        symlinks+=("$sym")
    done < <(find_symlinks_for_flat "$flat_path")

    if [[ ${#symlinks[@]} -eq 0 ]]; then
        warn "No symlinks found for $flat_path – will still delete the flat dir."
    else
        log "Found ${#symlinks[@]} symlink(s):"
        for sym in "${symlinks[@]}"; do
            log "  $sym"
        done
    fi

    # ------------------------------------------------------------------
    # 2. Delete the flat overlay from CVMFS
    # ------------------------------------------------------------------
    log "Deleting flat overlay ..."
    run cvmfs_server ingest --fast-delete "${flat_path}/"

    # ------------------------------------------------------------------
    # 3. For each image: delete from ducc, then reconvert
    # ------------------------------------------------------------------
    local had_error=false
    for sym in "${symlinks[@]}"; do
        local image_url
        image_url=$(symlink_to_image_url "$sym")
        log "----------------------------------------"
        log "Image: $image_url"

        log "  Deleting image record from CVMFS ..."
        if ! run cvmfs_ducc delete-image "$CVMFS_REPO_NAME" "$image_url"; then
            warn "delete-image failed for $image_url – continuing."
            had_error=true
        fi

        log "  Reconverting image ..."
        # shellcheck disable=SC2086
        if ! run cvmfs_ducc convert-single-image $CONVERT_EXTRA_ARGS "$image_url" "$CVMFS_REPO_NAME"; then
            warn "convert-single-image failed for $image_url"
            had_error=true
        fi
    done

    if $had_error; then
        warn "One or more operations failed for flat path: $flat_path"
        return 1
    fi

    log "Done: $flat_path"
}

# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------
overall_exit=0
for flat in "${FLAT_PATHS[@]}"; do
    if ! process_flat_path "$flat"; then
        overall_exit=1
    fi
done

if [[ $overall_exit -ne 0 ]]; then
    err "One or more flat paths had errors."
fi
exit $overall_exit
