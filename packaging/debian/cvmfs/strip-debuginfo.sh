#!/bin/sh
#
# Moves debug info into cvmfs-dbg using the standard build-ID layout.
# Unlike dh_strip, --strip-debug keeps .symtab for watchdog backtraces.
# Usage: strip-debuginfo.sh <cvmfs-dbg staging dir> <staging dir>...

set -e

if [ $# -lt 2 ]; then
  echo "usage: $0 <dbg staging dir> <staging dir>..." >&2
  exit 1
fi

dbg_dir="$1"
shift

filelist="$(mktemp)"
trap 'rm -f "$filelist"' EXIT

# Some configurations omit package staging directories.
: > "$filelist"
for dir in "$@"; do
  if [ -d "$dir" ]; then
    find "$dir" -type f -print >> "$filelist"
  else
    echo "strip-debuginfo: no staging directory $dir, skipping"
  fi
done

# Keep the loop in this shell so set -e applies.
while IFS= read -r file; do
  case "$(file -b "$file")" in
    *ELF*executable*|*ELF*shared\ object*) ;;
    *) continue ;;
  esac

  # Preserve Go runtime tracebacks.
  if readelf -SW "$file" | grep -q ' \.gopclntab'; then
    echo "strip-debuginfo: skipping Go binary $file"
    continue
  fi

  # Build ID follows the NT_GNU_BUILD_ID label.
  build_id="$(readelf -nW "$file" \
              | sed -n 's/.*Build ID:[[:space:]]*\([0-9a-f]\{16,\}\).*/\1/p' \
              | head -n 1)"
  if [ -z "$build_id" ]; then
    echo "strip-debuginfo: no build ID, not stripping $file" >&2
    continue
  fi

  # GDB finds this path from the build ID without .gnu_debuglink.
  debug_file="$dbg_dir/usr/lib/debug/.build-id/$(echo "$build_id" | cut -c1-2)"
  debug_file="$debug_file/$(echo "$build_id" | cut -c3-).debug"

  mkdir -p "$(dirname "$debug_file")"
  objcopy --only-keep-debug "$file" "$debug_file"
  chmod 644 "$debug_file"

  # Keep .symtab and preserve the executable mode.
  strip --strip-debug --remove-section=.comment "$file"
done < "$filelist"
