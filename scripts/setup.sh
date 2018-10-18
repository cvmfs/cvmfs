#!/bin/sh

#-------------------------------------------------------------------
#
# This file is part of the CernVM File System.
#
#-------------------------------------------------------------------

set -e

SCRIPT_LOCATION=$(cd "$(dirname "$0")"; pwd)

export RUNNER_LOG_DIR=/var/log/cvmfs-gateway-runner

# Install syslog configuration file
echo "Installing the syslog configuration file"
sudo cp -v $SCRIPT_LOCATION/90-cvmfs-gateway.conf /etc/rsyslog.d/

if [ x"$(which systemctl)" != x"" ]; then
    echo "  - restarting rsyslog"
    sudo cp -v $SCRIPT_LOCATION/90-cvmfs-gateway-rotate-systemd /etc/logrotate.d/
    sudo systemctl restart rsyslog

    echo "  - installing systemd service file"
    sudo cp -v $SCRIPT_LOCATION/cvmfs-gateway.service /etc/systemd/system/cvmfs-gateway.service
    sudo systemctl daemon-reload
else
    echo "  - restarting rsyslog"
    sudo cp -v $SCRIPT_LOCATION/90-cvmfs-gateway-rotate /etc/logrotate.d/
    sudo service rsyslog restart
fi

# Symlink the configuration directory into /etc/cvmfs/gateway
if [ ! -e /etc/cvmfs/gateway ]; then
    echo "Creating onfiguration file directory to /etc/cvmfs/gateway"
    sudo mkdir /etc/cvmfs/gateway
fi
if [ ! -e /etc/cvmfs/gateway/repo.json ]; then
    echo "Copying repo.json to /etc/cvmfs/gateway"
    sudo cp -v $SCRIPT_LOCATION/../etc/repo.json /etc/cvmfs/gateway/
fi
if [ ! -e /etc/cvmfs/gateway/user.json ]; then
    echo "Copying user.json to /etc/cvmfs/gateway"
    sudo cp -v $SCRIPT_LOCATION/../etc/user.json /etc/cvmfs/gateway/
fi

# Setup Mnesia
echo "Setting up the Mnesia"

cvmfs_mnesia_root=/var/lib/cvmfs-gateway
echo "  - (re)creating schema directory at $cvmfs_mnesia_root"
sudo rm -rf $cvmfs_mnesia_root && sudo mkdir -p $cvmfs_mnesia_root

echo "  - creating Mnesia schema"
sudo $SCRIPT_LOCATION/../bin/cvmfs_gateway escript scripts/setup_mnesia.escript $cvmfs_mnesia_root
