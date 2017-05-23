#!/bin/sh

#-------------------------------------------------------------------
#
# This file is part of the CernVM File System.
#
#-------------------------------------------------------------------

# Setup Mnesia
bin/cvmfs_services escript scripts/setup_mnesia.escript

# Install syslog configuration file
cp -v scripts/90-cvmfs_services.conf /etc/rsyslog.d/
systemctl restart rsyslog
