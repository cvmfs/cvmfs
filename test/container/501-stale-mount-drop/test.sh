#!/bin/bash
#
# Regression test for issue #3867: a gateway release manager whose read-only
# mount is behind HEAD must not drop content another release manager added.
#
# Deterministic, no timing races: pub1 is parked in transaction_before_hook
# (after it froze its manifest, before it takes the lease); pub2 advances HEAD
# in that window; pub1 is then released and publishes an unrelated change.
#
#   - With the fix:  pub1's post-lease refresh pulls in pub2's nested catalog
#                    -> it survives.
#   - Without it:    pub1 diffs a stale mount, old_root_hash already has the
#                    nested catalog -> it is reported as a deletion and dropped.
#
# Assumes the stack is already up:  docker compose up --build -d
set -euo pipefail

cd "$(dirname "$0")"
REPO=test.repo.org
DC="docker compose"

# exec helpers (no TTY, for CI)
ex()  { $DC exec -T "$@"; }                 # ex <service> <cmd...>
gw()  { ex gw1  "$@"; }
p1()  { ex pub1 "$@"; }
p2()  { ex pub2 "$@"; }

wait_for() {  # wait_for <desc> <max_tries> <service> <cmd...>
    local desc=$1 tries=$2; shift 2
    for ((i=1; i<=tries; i++)); do
        if ex "$@" >/dev/null 2>&1; then return 0; fi
        sleep 1
    done
    echo "FATAL: timed out waiting for: $desc"; return 1
}

echo "=== 1. set up gateway (Stratum-0 + cvmfs-gateway) ==="
gw /scripts/setup_gateway.sh
wait_for "httpd serving the repo" 60 gw1 \
    curl -sf http://localhost/cvmfs/$REPO/.cvmfspublished
wait_for "gateway API on :4929" 60 gw1 \
    curl -sf http://localhost:4929/api/v1/repos

echo "=== 2. attach the two release managers ==="
p1 /scripts/setup_publisher.sh
p2 /scripts/setup_publisher.sh

echo "=== 3. baseline: pub1 creates /dir/keep (HEAD = H0) ==="
p1 cvmfs_server transaction $REPO
p1 bash -c "mkdir -p /cvmfs/$REPO/dir && echo keep > /cvmfs/$REPO/dir/keep"
p1 cvmfs_server publish $REPO

echo "=== 4. arm the deterministic barrier on pub1 ==="
p1 cp /scripts/pub1_hooks.sh /etc/cvmfs/cvmfs_server_hooks.sh
p1 bash -c 'rm -f /tmp/pub1_parked /tmp/release_fifo /tmp/arm_barrier;
            mkfifo /tmp/release_fifo; touch /tmp/arm_barrier'

echo "=== 5. start pub1 transaction on /dir; it parks in the hook ==="
# Run the (blocking) transaction in a host-side background job. The hook keeps
# it parked until we write to the FIFO; pub1's manifest is already frozen at H0.
( p1 cvmfs_server transaction $REPO/dir ) &
txn_job=$!
wait_for "pub1 to park in transaction_before_hook" 60 pub1 test -f /tmp/pub1_parked

echo "=== 6. pub2 publishes sibling nested catalog dir/sub (HEAD -> H1) ==="
p2 cvmfs_server transaction $REPO/dir
p2 bash -c "mkdir -p /cvmfs/$REPO/dir/sub &&
            touch /cvmfs/$REPO/dir/sub/.cvmfscatalog &&
            echo payload > /cvmfs/$REPO/dir/sub/payload"
p2 cvmfs_server publish $REPO

echo "=== 7. release pub1; it acquires the lease and must refresh its mount ==="
p1 bash -c 'echo go > /tmp/release_fifo'
wait $txn_job   # transaction command returns once the lease is held

echo "=== 8. pub1 publishes an unrelated change under the same lease ==="
p1 bash -c "echo other > /cvmfs/$REPO/dir/other"
p1 cvmfs_server publish $REPO

echo "=== 9. verify the published HEAD ==="
# Inspect the *published* repository, not pub1's local read-only mount. A gateway
# publish does NOT fast-forward the publisher's local mount to the just-committed
# revision: cvmfs_server_publish.sh returns early for `gw` upstreams (never
# reaching set_ro_root_hash), and close_transaction remounts rdonly at the
# unchanged CVMFS_ROOT_HASH -- i.e. the pre-publish base the transaction was
# refreshed to. That base holds `keep` and pub2's `sub` but NOT pub1's own
# freshly-committed `other`, so checking the local mount would spuriously fail.
# Opening a fresh transaction refreshes rdonly to HEAD (the #3867 fix); we
# inspect there and abort to restore a clean state.
p1 cvmfs_server transaction $REPO
echo "--- pub1 view of /$REPO/dir at HEAD ---"
p1 bash -c "ls -la /cvmfs/$REPO/dir/ /cvmfs/$REPO/dir/sub/ 2>&1" || true
gw cvmfs_server tag -l $REPO 2>/dev/null || true

rc=0
p1 test -e /cvmfs/$REPO/dir/sub/payload \
    || { echo "FAIL: dir/sub/payload was DROPPED -> bug present"; rc=1; }
p1 test -e /cvmfs/$REPO/dir/other \
    || { echo "FAIL: dir/other (pub1's own change) missing";       rc=1; }
p1 test -e /cvmfs/$REPO/dir/keep \
    || { echo "FAIL: dir/keep (baseline) missing";                 rc=1; }

p1 cvmfs_server abort -f $REPO || true

if [ $rc -eq 0 ]; then
    echo "PASS: concurrently-added nested catalog preserved across uncontended publish"
fi
exit $rc
