#!/bin/sh
# Resolve the CernVM-FS version for builds and packaging.
#
# Resolution order:
#   1. CVMFS_VERSION_OVERRIDE
#   2. CVMFS_VERSION embedded in a source archive
#   3. an exact cvmfs-X.Y.Z[-preN] Git tag
#   4. a reproducible development version derived from Git
#   5. the base version from VERSION
#
# Development versions have the form
#   X.Y.Z~dev<order>.<sequence>.<UTC commit timestamp>.git<short hash>[.dirty]
# CVMFS_VERSION_ORDER defaults to 1 (0 is reserved for deliberately lower
# builds), CVMFS_VERSION_SEQUENCE defaults to 0, and CVMFS_VERSION_TIMESTAMP
# can override the timestamp when necessary.
set -e

source_directory=${1:-$(pwd)}
source_directory=$(cd "$source_directory" && pwd -P)

fail() {
  echo "get_version.sh: $*" >&2
  exit 1
}

validate_version() {
  echo "$1" | grep -Eq '^[0-9]+\.[0-9]+\.[0-9]+([~+][0-9A-Za-z.+~]+)?$' \
    || fail "invalid version '$1'"
}

if [ -n "${CVMFS_VERSION_OVERRIDE:-}" ]; then
  validate_version "$CVMFS_VERSION_OVERRIDE"
  printf '%s\n' "$CVMFS_VERSION_OVERRIDE"
  exit 0
fi

# Source archives contain the version resolved when the archive was created.
if [ -f "$source_directory/CVMFS_VERSION" ]; then
  version=$(sed -n '1p' "$source_directory/CVMFS_VERSION")
  validate_version "$version"
  printf '%s\n' "$version"
  exit 0
fi

[ -f "$source_directory/VERSION" ] \
  || fail "cannot find $source_directory/VERSION"
base_version=$(sed -n '1p' "$source_directory/VERSION")
echo "$base_version" | grep -Eq '^[0-9]+\.[0-9]+\.[0-9]+$' \
  || fail "invalid base version '$base_version'"

# Do not accidentally use a parent repository when building an unpacked archive.
if command -v git >/dev/null 2>&1; then
  git_root=$(git -C "$source_directory" rev-parse --show-toplevel 2>/dev/null || true)
else
  git_root=
fi

if [ -n "$git_root" ]; then
  git_root=$(cd "$git_root" && pwd -P)
fi

if [ "$git_root" != "$source_directory" ]; then
  printf '%s\n' "$base_version"
  exit 0
fi

dirty_suffix=
if ! git -C "$source_directory" diff --quiet HEAD --; then
  dirty_suffix=.dirty
fi

exact_tag=$(git -C "$source_directory" describe --tags --exact-match \
  --match 'cvmfs-[0-9]*' HEAD 2>/dev/null || true)
if [ -n "$exact_tag" ] && [ -z "$dirty_suffix" ]; then
  version=${exact_tag#cvmfs-}
  case "$version" in
    *-pre[0-9]*) version=$(echo "$version" | sed 's/-pre/~pre/') ;;
  esac
  validate_version "$version"
  printf '%s\n' "$version"
  exit 0
fi

order=${CVMFS_VERSION_ORDER:-1}
sequence=${CVMFS_VERSION_SEQUENCE:-0}
timestamp=${CVMFS_VERSION_TIMESTAMP:-$(
  TZ=UTC git -C "$source_directory" show -s \
    --date=format-local:%Y%m%d%H%M%S --format=%cd HEAD
)}
short_hash=$(git -C "$source_directory" rev-parse --short=10 HEAD)

echo "$order" | grep -Eq '^[0-9]+$' \
  || fail "CVMFS_VERSION_ORDER must be numeric"
echo "$sequence" | grep -Eq '^[0-9]+$' \
  || fail "CVMFS_VERSION_SEQUENCE must be numeric"
echo "$timestamp" | grep -Eq '^[0-9]+$' \
  || fail "CVMFS_VERSION_TIMESTAMP must be numeric"

version="${base_version}~dev${order}.${sequence}.${timestamp}.git${short_hash}${dirty_suffix}"
validate_version "$version"
printf '%s\n' "$version"
