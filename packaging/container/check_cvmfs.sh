#!/bin/sh

# CernVM-FS service container health check

eval $(/usr/bin/cvmfs2 -o parse dummy.cvmfs.io / | grep ^CVMFS_REPOSITORIES=)
echo "[INF] checking $CVMFS_REPOSITORIES"

check_readiness() {
  for r in $(echo "$CVMFS_REPOSITORIES" | tr , ' '); do
    mountpoint /cvmfs/$r || return 1
  done
  return 0
}

check_liveness() {
  for r in $(echo "$CVMFS_REPOSITORIES" | tr , ' '); do
    mountpoint /cvmfs/$r || return 1
    ls /cvmfs/$r >/dev/null || return 1
  done
  return 0
}

check_$@

