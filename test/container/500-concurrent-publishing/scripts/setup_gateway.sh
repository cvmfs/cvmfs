#!/bin/bash
echo "plain_text mykey mysecret" > /etc/cvmfs/keys/test.repo.org.gw
systemctl start httpd
cvmfs_server mkfs -o root  test.repo.org
systemctl start cvmfs-gateway
