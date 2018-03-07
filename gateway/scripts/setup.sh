#!/bin/sh

#-------------------------------------------------------------------
#
# This file is part of the CernVM File System.
#
#-------------------------------------------------------------------

set -e

SCRIPT_LOCATION=$(cd "$(dirname "$0")"; pwd)

# Setup Mnesia
echo "Setting up the Mnesia"

cvmfs_mnesia_root=/opt/cvmfs_mnesia
echo "  - (re)creating schema directory at $cvmfs_mnesia_root"
sudo rm -rf $cvmfs_mnesia_root && sudo mkdir -p $cvmfs_mnesia_root

echo "  - creating Mnesia schema"
sudo $SCRIPT_LOCATION/../bin/cvmfs_gateway escript scripts/setup_mnesia.escript $cvmfs_mnesia_root

# Install syslog configuration file
echo "Installing the syslog configuration file"
sudo cp -v $SCRIPT_LOCATION/90-cvmfs_gateway.conf /etc/rsyslog.d/

# Symlink the configuration directory into /etc/cvmfs/gateway
if [ ! -f /etc/cvmfs/gateway ]; then
    echo "Symlinking configuration file directory to /etc/cvmfs/gateway"
    sudo ln -s $(readlink -f $SCRIPT_LOCATION/../etc) /etc/cvmfs/gateway
else
    echo "The file/symlink \"/etc/cvmfs/gateway\" already exists."
fi

if [ x"$(which systemctl)" != x"" ]; then
    echo "  - restarting rsyslog"
    sudo systemctl restart rsyslog
    sudo cp -v $SCRIPT_LOCATION/90-cvmfs_gateway_rotate_systemd /etc/logrotate.d/

    echo "  - installing systemd service file"
    sudo cp -v $SCRIPT_LOCATION/cvmfs_gateway.service /etc/systemd/system/cvmfs_gateway.service
else
    echo "  - restarting rsyslog"
    sudo service rsyslog restart
    sudo cp -v $SCRIPT_LOCATION/90-cvmfs_gateway_rotate /etc/logrotate.d/
fi

