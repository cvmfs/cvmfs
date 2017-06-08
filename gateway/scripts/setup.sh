#!/bin/sh

#-------------------------------------------------------------------
#
# This file is part of the CernVM File System.
#
#-------------------------------------------------------------------

set -e

# Setup Mnesia
echo "Setting up the Mnesia"

cvmfs_mnesia_root=/opt/cvmfs_mnesia
echo "  - creating schema directory at $cvmfs_mnesia_root"
sudo mkdir -p $cvmfs_mnesia_root
sudo chown -R `whoami`:`whoami` $cvmfs_mnesia_root

echo "  - creating Mnesia schema"
bin/cvmfs_services escript scripts/setup_mnesia.escript $cvmfs_mnesia_root

# Install syslog configuration file
echo "Installing the syslog configuration file"
sudo cp -v scripts/90-cvmfs_services.conf /etc/rsyslog.d/

echo "  - restarting rsyslog"
if [ x"$(which systemctl)" != x"" ]; then
    sudo systemctl restart rsyslog
else
    sudo service rsyslog restart
fi

