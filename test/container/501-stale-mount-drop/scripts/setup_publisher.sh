#!/bin/bash
# Attach a connected release manager to the gateway (gw upstream).
set -e

FQRN="${FQRN:-test.repo.org}"
GW="${GW:-http://cvmfs-gw1}"

cvmfs_server mkfs \
    -w "$GW/cvmfs/$FQRN" \
    -u "gw,/srv/cvmfs/$FQRN/data/txn,$GW:4929/api/v1" \
    -k /etc/cvmfs/keys \
    -o "$(whoami)" \
    "$FQRN"

echo "[setup_publisher] $(hostname) attached to $GW"
