# /etc/cvmfs/cvmfs_server_hooks.sh on pub1 (test instrumentation only).
#
# Deterministic barrier: when armed, transaction_before_hook parks the
# transaction *after* the publisher has frozen its manifest (constructor) but
# *before* it acquires the lease and runs the post-lease mount refresh. That is
# exactly the window in which pub2 advances HEAD, so without the fix pub1 ends
# up diffing a stale read-only mount.
#
# The driver (test.sh) arms a single transaction by creating /tmp/arm_barrier
# and releases it by writing to the FIFO /tmp/release_fifo. Both files must
# already exist before the armed transaction starts.
transaction_before_hook() {
    [ -f /tmp/arm_barrier ] || return 0   # only the armed transaction blocks
    rm -f /tmp/arm_barrier                # one-shot
    touch /tmp/pub1_parked                # signal the driver we are parked
    cat /tmp/release_fifo > /dev/null     # block (no spin) until released
    rm -f /tmp/pub1_parked
    return 0
}
