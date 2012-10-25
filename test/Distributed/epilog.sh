#!/bin/bash

# This script will install cvmfs-test on a machine if it's not yet installed.
# If it is, it will start the daemon in order to communicate with another shell.
# Everytime you use it to contextualize a CernVM, you should change the SHELLPATH variable
# to match your address, cvmfs-test will do it for you.

CVMFSTESTBIN="/usr/bin/git"
GITBIN="/usr/bin/cvmfstest"
SHELLPATH="192.168.1.50"
LOGFILE="/root/initialization.log"

echo "Running initialization script..." > $LOGFILE

if [ ! "$CVMFSTESTBIN" == "" ] ; then
	echo "cvmfs-test binary found. Starting it."
	# Starting the daemon
	$CVMFSTESTBIN --start --shell-path $SHELLPATH:6651 --iface eth1
else
	echo "cvmfs-test binary not found. Starting installation process." >> $LOGIFILE
	# Checking if git is already installed. It's not shipped with CernVM by default.
	# If it's not yet installed, the script will install it as it needs it to retrieve
	# the source code of cvmfs-test.
	if [ "$GITBIN" == "" ] ; then
		echo "Installing git..." >> $LOGFILE
		conary update git
	fi
	
	# Changing current directory to root home
	cd /root
	
	# Cloning the repo ad installing the software
	echo "Cloning cvmfs-test repository..." >> $LOGFILE
	$GITBIN clone git://github.com/ruvolof/cvmfs-test cvmfs-test
	./cvmfs-test/Install.pl
	
	# Starting the daemon
	echo "Starting the daemon." >> $LOGFILE
	$CVMFSTESTBIN --start --shell-path $SHELLPATH --iface eth1
fi

exit 0
