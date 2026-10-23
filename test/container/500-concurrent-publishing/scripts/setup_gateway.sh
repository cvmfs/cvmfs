#!/bin/bash
# setup_gateway.sh – initialise the gateway Stratum-0 inside a container.
#
# Set USE_SYSTEMCTL=1 to manage services via systemd (the default when the
# container runs /usr/sbin/init as PID 1).  In minimal containers that start
# the daemon directly (e.g. the publish/ compose stack) leave it unset or
# set to 0: the script will skip systemctl calls and rely on the process
# already being started by the container entrypoint.

set -e

FQRN="${FQRN:-test.repo.org}"
USE_SYSTEMCTL="${USE_SYSTEMCTL:-0}"

# ---------------------------------------------------------------------------
# Helper: conditionally call systemctl
# ---------------------------------------------------------------------------
maybe_systemctl() {
    if [ "${USE_SYSTEMCTL}" = "1" ]; then
        while ! systemctl status &>/dev/null; do sleep 0.1; done
        systemctl "$@"
    else
        echo "[setup_gateway] Skipping: systemctl $* (USE_SYSTEMCTL not set)"
    fi
}

# Write the gateway lease key
echo "plain_text mykey mysecret" > /etc/cvmfs/keys/${FQRN}.gw

# Start httpd before mkfs so Apache is available for the health-check
# (only needed when using local upstream storage; skipped with S3).
maybe_systemctl start httpd

# Create the Stratum-0 repository (local upstream, Apache serves /srv/cvmfs)
cvmfs_server mkfs -o root "${FQRN}"

# Start the gateway service
maybe_systemctl start cvmfs-gateway
