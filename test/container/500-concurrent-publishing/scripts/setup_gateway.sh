#!/bin/bash
echo "plain_text mykey mysecret" > /etc/cvmfs/keys/test.repo.org.gw
while ! systemctl status &>/dev/null; do sleep 0.1; done # avoid Failed to connect to bus
systemctl start httpd
cvmfs_server mkfs -o root  test.repo.org
systemctl start cvmfs-gateway
