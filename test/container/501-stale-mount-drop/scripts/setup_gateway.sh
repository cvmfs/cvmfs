#!/bin/bash
# Initialise the Stratum-0 + gateway inside the gw1 container.
set -e

FQRN="${FQRN:-test.repo.org}"

# Wait until systemd is up enough to accept systemctl commands.
while ! systemctl status &>/dev/null; do sleep 0.2; done

# Gateway lease key (shared with publishers via the `keys` volume).
echo "plain_text mykey mysecret" > /etc/cvmfs/keys/${FQRN}.gw

# Apache serves the Stratum-0 data to the connected publishers.
systemctl start httpd

# Local-upstream Stratum-0.
cvmfs_server mkfs -o root "${FQRN}"

# Make the public + gateway keys readable by the publishers.
chmod 0644 /etc/cvmfs/keys/${FQRN}.pub /etc/cvmfs/keys/${FQRN}.gw

# Gateway service: mediates leases and spawns cvmfs_receiver.
systemctl start cvmfs-gateway

echo "[setup_gateway] done"
