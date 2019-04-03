#!/bin/sh

#-------------------------------------------------------------------
#
# This file is part of the CernVM File System.
#
#-------------------------------------------------------------------

set -e

export RUNNER_LOG_DIR=/var/log/cvmfs-gateway-runner
export CVMFS_GATEWAY_ROOT=/usr/libexec/cvmfs-gateway

# reload systemd services
systemctl daemon-reload

# restarting rsyslog
systemctl restart rsyslog

# config files
if [ ! -e /etc/cvmfs/gateway ]; then
    mkdir -p /etc/cvmfs/gateway
fi
if [ ! -e /etc/cvmfs/gateway/repo.json ]; then
    cp $CVMFS_GATEWAY_ROOT/etc/repo.json /etc/cvmfs/gateway/
fi
if [ ! -e /etc/cvmfs/gateway/user.json ]; then
    cp $CVMFS_GATEWAY_ROOT/etc/user.json /etc/cvmfs/gateway/
fi

# Setup Mnesia
cvmfs_mnesia_root=/var/lib/cvmfs-gateway
rm -rf $cvmfs_mnesia_root && mkdir -p $cvmfs_mnesia_root
$CVMFS_GATEWAY_ROOT/bin/cvmfs_gateway escript scripts/setup_mnesia.escript $cvmfs_mnesia_root
