#!/bin/bash
#
# Mounts sft.cern.ch with `cvmfs2 -o nfs` (kernel NFS client, no macFUSE),
# sources an LCG view and runs ROOT from it.
#
# Usage: sudo macos_lcg_test.sh <cvmfs build dir> <LCG platform> [workdir]
#   e.g. sudo macos_lcg_test.sh build/cvmfs arm64-mac15-clang170-opt
#
# Every step has a time limit, and diagnostics are printed on failure, so that
# a hanging mount cannot block the caller.

set -u

BUILD=$(cd "$1" && pwd)
PLATFORM=$2
WORK=${3:-/var/tmp/cvmfs-nfs-lcg}
REPO=sft.cern.ch
LCG_RELEASE=${LCG_RELEASE:-LCG_110a}
MNT=/cvmfs/$REPO
SRC_ROOT=$(cd "$(dirname "$0")/../.." && pwd)
RUN_AS=${SUDO_USER:-$(id -un)}

CACHE=$WORK/cache
TRACE=$WORK/nfs4-trace.log
CONF=$WORK/$REPO.conf
TALK=$CACHE/$REPO/cvmfs_io.$REPO

limit() {
  local secs=$1; shift
  perl -e 'alarm shift; exec @ARGV' "$secs" "$@"
}

diagnostics() {
  echo "::group::diagnostics"
  mount | grep -E "nfs|$REPO"
  ps -axo pid,ppid,stat,etime,command | grep -E "cvmfs2|mount_nfs" | grep -v grep
  echo "--- NFS trace (last 40 lines of $(wc -l < "$TRACE" 2>/dev/null) total)"
  tail -40 "$TRACE" 2>/dev/null
  echo "--- delayed requests: $(grep -c delayed "$TRACE" 2>/dev/null)"
  for f in "$CACHE/$REPO"/stacktrace.*; do
    [ -f "$f" ] && { echo "--- $f"; head -c 20000 "$f"; }
  done
  echo "--- system log"
  log show --last 15m --style compact \
    --predicate 'process == "cvmfs2" OR process == "mount_nfs" OR sender == "nfs"' \
    2>/dev/null | grep -viE "dyld|libsystem_info" | tail -60
  echo "::endgroup::"
}

# Unmount while the server is still alive: with processes blocked on the mount,
# killing the server first can wedge the whole system on macOS
cleanup() {
  pkill -9 -f "root.exe" 2>/dev/null
  if mount | grep -q "$REPO"; then
    limit 60 umount -f "$MNT" 2>/dev/null || limit 60 umount -f "$MNT" 2>/dev/null
  fi
  pkill -f "cvmfs2 -o nfs" 2>/dev/null
}

fail() {
  echo "FAIL: $*"
  diagnostics
  cleanup
  exit 1
}

rm -rf "$WORK"
mkdir -p "$CACHE"
cat > "$CONF" <<EOF
CVMFS_HTTP_PROXY=${CVMFS_HTTP_PROXY:-DIRECT}
CVMFS_SERVER_URL='http://s1cern-cvmfs.openhtc.io/cvmfs/@fqrn@;http://s1ral-cvmfs.openhtc.io/cvmfs/@fqrn@;http://s1bnl-cvmfs.openhtc.io/cvmfs/@fqrn@;http://s1fnal-cvmfs.openhtc.io/cvmfs/@fqrn@'
CVMFS_USE_GEOAPI=yes
CVMFS_KEYS_DIR=$SRC_ROOT/mount/keys
CVMFS_CACHE_BASE=$CACHE
CVMFS_RELOAD_SOCKETS=$CACHE
CVMFS_SHARED_CACHE=no
CVMFS_QUOTA_LIMIT=6000
CVMFS_USER=root
CVMFS_CHECK_PERMISSIONS=yes
CVMFS_CLAIM_OWNERSHIP=yes
EOF

[ -d "$MNT" ] || fail "mount point $MNT does not exist"
cleanup

echo "::group::mount"
export CVMFS_LIBRARY_PATH=$BUILD CVMFS_NFS4_DEBUG=$TRACE
export CVMFS_NFS_DELEGATIONS
t0=$(date +%s)
limit 300 "$BUILD/cvmfs2" -o "nfs,config=$CONF" "$REPO" "$MNT" \
  || fail "cvmfs2 -o nfs exited with $?"
echo "mounted in $(( $(date +%s) - t0 ))s"
mount | grep "$REPO" || fail "not mounted"
nfsstat -m "$MNT" 2>/dev/null | head -5
limit 120 ls "$MNT" || fail "cannot list $MNT"
echo "::endgroup::"

echo "::group::LCG $LCG_RELEASE/$PLATFORM"
VIEW=$MNT/lcg/views/$LCG_RELEASE/$PLATFORM
cat > "$WORK/t.C" <<'EOF'
void t() {
  TH1F h("h", "gaus", 100, -5, 5);
  h.FillRandom("gaus", 100000);
  TF1 f("f", "gaus", -5, 5);
  h.Fit(&f, "Q");
  printf("ROOT %s: fitted sigma = %.3f\n", gROOT->GetVersion(), f.GetParameter(2));
}
EOF
chmod -R a+rX "$WORK"
cat > "$WORK/run_root.sh" <<EOF
set -e
source $VIEW/setup.sh
which root
for i in 1 2; do
  t0=\$(date +%s)
  r0=\$(grep -c " op 25 " $TRACE)
  root -l -b -q $WORK/t.C
  echo "root run \$i: \$(( \$(date +%s) - t0 ))s, \$(( \$(grep -c " op 25 " $TRACE) - r0 )) READs"
  vm_stat | grep -E "Pages (free|active|inactive|purgeable)|File-backed"
done
python3 -c 'import ROOT; print("PyROOT", ROOT.gROOT.GetVersion())'
EOF
limit 1800 sudo -u "$RUN_AS" bash "$WORK/run_root.sh" 2>&1 | tee "$WORK/root.log"
grep -q "fitted sigma = 1.00\|fitted sigma = 0.99" "$WORK/root.log" \
  || fail "ROOT fit did not run"
grep -q "^PyROOT " "$WORK/root.log" || fail "PyROOT import failed"
echo "::endgroup::"

echo "::group::extended attributes (informational)"
xattr -p user.revision "$MNT" 2>&1
xattr -p user.fqrn "$MNT" 2>&1
echo "::endgroup::"

echo "::group::unmount"
PID=$(limit 30 "$BUILD/cvmfs_talk" -p "$TALK" pid 2>/dev/null)
[ -n "$PID" ] || fail "cvmfs_talk does not answer"
kill "$PID"
for _ in $(seq 30); do
  mount | grep -q "$REPO" || break
  sleep 1
done
mount | grep -q "$REPO" && fail "still mounted after SIGTERM"
echo "::endgroup::"

echo "::group::guardian: kill -9 cvmfs2"
limit 300 "$BUILD/cvmfs2" -o "nfs,config=$CONF" "$REPO" "$MNT" \
  || fail "remount failed"
limit 60 ls "$MNT" > /dev/null || fail "cannot list $MNT after remount"
PID=$(limit 30 "$BUILD/cvmfs_talk" -p "$TALK" pid 2>/dev/null)
[ -n "$PID" ] || fail "cvmfs_talk does not answer after remount"
kill -9 "$PID"
t0=$(date +%s)
limit 60 cat "$MNT/README.md" > /dev/null 2>&1 && fail "read succeeded after kill -9"
echo "read after kill -9 failed after $(( $(date +%s) - t0 ))s"
for _ in $(seq 60); do
  mount | grep -q "$REPO" || break
  sleep 1
done
mount | grep -q "$REPO" && fail "guardian did not unmount after kill -9"
echo "unmounted $(( $(date +%s) - t0 ))s after kill -9"
for _ in $(seq 10); do
  pgrep -f __nfs_guardian__ > /dev/null || break
  sleep 1
done
pgrep -f __nfs_guardian__ && fail "guardian still running"
echo "::endgroup::"

echo "--- delegations: $(grep -c 'callback path.*works' "$TRACE") working callback paths, $(grep -c ' op 18 -> 0' "$TRACE") OPENs"
echo "--- delayed requests during the run: $(grep -c delayed "$TRACE")"
echo "PASS"
