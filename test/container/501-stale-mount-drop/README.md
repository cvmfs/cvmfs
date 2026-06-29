# 501 - gateway stale-mount content drop (issue #3867)

Regression test for the bug where a gateway release manager whose read-only
mount is behind HEAD drops content that another release manager added.

The failure is reproduced **deterministically** (no `sleep`/`-t` hammering) using
the existing `transaction_before_hook` as a barrier: pub1 is parked right after
it freezes its manifest but before it acquires the lease, pub2 advances HEAD in
that window, then pub1 is released and publishes an unrelated change. Without the
fix pub1 diffs its stale mount and the gateway records pub2's nested catalog as a
deletion; with the fix pub1's post-lease mount refresh pulls it in first.

```sh
docker compose up --build -d
./test.sh
docker compose down -v --remove-orphans
```

`test.sh` exits non-zero (and prints `FAIL: dir/sub/payload was DROPPED`) on a
buggy build, and prints `PASS` once the publisher refreshes its mount on every
gateway transaction.

Stack: `gw1` (Stratum-0 + cvmfs-gateway, local Apache upstream), `pub1` (victim),
`pub2` (advances HEAD). All built from the working tree so `pub1` exercises the
fix. Wired into CI by `.github/workflows/ci_stale_mount.yml`.
