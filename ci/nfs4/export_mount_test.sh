#!/bin/bash
#
# Serves a directory with cvmfs_nfs4_export and mounts it with the kernel NFS
# client (macOS or Linux), then compares the mount with the source.
#
# Usage: export_mount_test.sh <path to cvmfs_nfs4_export> <source tree to copy>
#
# Needs passwordless sudo for mount/umount.  Checks that are expected to fail
# on some client versions (xattrs, owner mapping) are reported, not enforced.

set -u

EXPORT_BIN=$(cd "$(dirname "$1")" && pwd)/$(basename "$1")
TREE=$2

WORK=$(mktemp -d)
SRC=$WORK/src
MNT=$WORK/mnt
SERVER_PID=
FAILURES=0

fail() {
  echo "FAIL: $*"
  FAILURES=$((FAILURES + 1))
}

cleanup() {
  sudo umount -f "$MNT" 2>/dev/null
  [ -n "$SERVER_PID" ] && kill "$SERVER_PID" 2>/dev/null
}
trap cleanup EXIT

start_server() {
  local port=$1
  : > "$WORK/port"
  CVMFS_NFS4_DEBUG=$WORK/trace.log \
    "$EXPORT_BIN" -p "$port" "$SRC" > "$WORK/port" 2> "$WORK/server.err" &
  SERVER_PID=$!
  for _ in $(seq 50); do
    [ -s "$WORK/port" ] && break
    sleep 0.1
  done
  PORT=$(cat "$WORK/port")
  [ -n "$PORT" ] || { cat "$WORK/server.err"; echo "server did not start"; exit 1; }
}

# Runs a command with a time limit; macOS has no timeout(1) by default
limit() {
  local secs=$1; shift
  perl -e 'alarm shift; exec @ARGV' "$secs" "$@"
}

if [ "$(uname)" = "Darwin" ]; then
  PLATFORM=macos
  stat_fmt() { stat -f '%N %HT %Lp %z %u %g %Y' "$@"; }
  set_xattr() { xattr -w "$2" "$3" "$1"; }
  get_xattr() { xattr -p "$2" "$1"; }
  SUM=shasum
else
  PLATFORM=linux
  stat_fmt() { stat -c '%n %F %a %s %u %g %N' "$@"; }
  set_xattr() { setfattr -n "$2" -v "$3" "$1"; }
  get_xattr() { getfattr --only-values -n "$2" "$1"; }
  SUM=sha1sum
fi

mkdir -p "$SRC" "$MNT"
cp -R "$TREE" "$SRC/tree"
ln -s tree/CMakeLists.txt "$SRC/link"
dd if=/dev/urandom of="$SRC/big.bin" bs=1048576 count=64 2>/dev/null
set_xattr "$SRC/big.bin" user.revision 42 || echo "note: cannot set xattr on source"

start_server 0
echo "server listening on port $PORT"

if [ $PLATFORM = macos ]; then
  MOUNT_OPTS="vers=4,tcp,port=$PORT,rdonly,intr,deadtimeout=60,timeo=100,retrans=2,nobrowse,namedattr"
  limit 60 sudo mount_nfs -o "$MOUNT_OPTS" 127.0.0.1:/ "$MNT" || {
    cat "$WORK/server.err"; echo "mount failed"; exit 1; }
  nfsstat -m "$MNT" || true
else
  MOUNT_OPTS="vers=4.0,port=$PORT,ro,soft,timeo=100,retrans=2"
  limit 60 sudo mount -t nfs4 -o "$MOUNT_OPTS" 127.0.0.1:/ "$MNT" || {
    cat "$WORK/server.err"; echo "mount failed"; exit 1; }
fi
mount | grep "$MNT"

echo "--- content"
limit 300 diff -r "$SRC" "$MNT" > "$WORK/diff.out" 2>&1 || {
  head -20 "$WORK/diff.out"; fail "content differs"; }

echo "--- metadata"
(cd "$SRC" && find . | sort | while read -r f; do stat_fmt "$f"; done) \
  > "$WORK/meta.src"
(cd "$MNT" && limit 300 find . | sort | while read -r f; do stat_fmt "$f"; done) \
  > "$WORK/meta.mnt"
if ! diff "$WORK/meta.src" "$WORK/meta.mnt" > "$WORK/meta.diff"; then
  head -20 "$WORK/meta.diff"
  # Owner mapping of NFSv4 owner strings is a known open question on macOS
  if diff <(awk '{$5=$6=""; print}' "$WORK/meta.src") \
          <(awk '{$5=$6=""; print}' "$WORK/meta.mnt") > /dev/null; then
    echo "NOTE: only uid/gid differ (owner string mapping)"
  else
    fail "metadata differs"
  fi
fi
echo "$(wc -l < "$WORK/meta.src") entries compared"

echo "--- parallel reads"
(cd "$SRC" && find . -type f | sort) > "$WORK/files"
(cd "$SRC" && xargs $SUM < "$WORK/files" | sort) > "$WORK/sum.src"
[ "$(wc -l < "$WORK/sum.src")" -eq "$(wc -l < "$WORK/files")" ] || fail "checksumming the source"
READERS=
for i in 1 2 3 4; do
  (cd "$MNT" && xargs -P 8 -n 20 $SUM < "$WORK/files" | sort > "$WORK/sum.$i") &
  READERS="$READERS $!"
done
wait $READERS
for i in 1 2 3 4; do
  cmp -s "$WORK/sum.src" "$WORK/sum.$i" || fail "parallel read pass $i"
done

echo "--- read throughput"
dd if="$MNT/big.bin" of=/dev/null bs=1048576 2>&1 | tail -1

# Whether the client keeps file data cached across processes: a second read
# of a file that did not change should not send READs (informational)
reads() { grep -c " op 25 " "$WORK/trace.log"; }
for mode in read mmap; do
  f=$MNT/tree/CMakeLists.txt
  [ $mode = mmap ] && f=$MNT/big.bin
  r0=$(reads)
  if [ $mode = read ]; then cat "$f" > /dev/null; else
    python3 -c "import mmap,sys; f=open(sys.argv[1],'rb'); m=mmap.mmap(f.fileno(),0,prot=mmap.PROT_READ); m[:].count(b'x')" "$f"; fi
  r1=$(reads)
  if [ $mode = read ]; then cat "$f" > /dev/null; else
    python3 -c "import mmap,sys; f=open(sys.argv[1],'rb'); m=mmap.mmap(f.fileno(),0,prot=mmap.PROT_READ); m[:].count(b'x')" "$f"; fi
  r2=$(reads)
  echo "cache check ($mode): first pass $((r1 - r0)) READs, second pass $((r2 - r1)) READs"
done

echo "--- symlink"
[ "$(readlink "$MNT/link")" = "tree/CMakeLists.txt" ] || fail "readlink"

echo "--- extended attributes (informational)"
if value=$(get_xattr "$MNT/big.bin" user.revision 2>&1); then
  echo "user.revision=$value"
  [ "$value" = 42 ] || echo "NOTE: unexpected xattr value"
else
  echo "NOTE: xattr not readable over NFS: $value"
fi

echo "--- read-only"
touch "$MNT/newfile" 2>/dev/null && fail "write succeeded"
rm "$MNT/big.bin" 2>/dev/null && fail "unlink succeeded"

echo "--- server restart"
kill "$SERVER_PID"
wait "$SERVER_PID" 2>/dev/null
start_server "$PORT"
if limit 120 ls "$MNT/tree" > /dev/null; then
  limit 120 cmp -s "$SRC/tree/CMakeLists.txt" "$MNT/tree/CMakeLists.txt" \
    || fail "read after restart"
else
  fail "listing after restart"
fi

echo "--- unmount"
limit 60 sudo umount "$MNT" || fail "umount"

echo "--- server log"
cat "$WORK/server.err"

if [ $FAILURES -gt 0 ]; then
  echo "$FAILURES check(s) failed"
  exit 1
fi
echo "all checks passed"
