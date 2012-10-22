#!/bin/bash

# This script will install cvmfs-test on a machine if it's not yet installed.
# If it is, it will start the daemon in order to communicate with another shell.
# Everytime you use it to contextualize a CernVM, you should change the SHELLPATH variable
# to match your address.

CVMFSTESTBIN=`which cvmfs-test 2> /dev/null`
GITBIN=`which git 2> /dev/null`
SHELLPATH="192.168.1.50"

if [ ! "$CVMFSTESTBIN" == "" ] ; then
	# Starting the daemon
	$CVMFSTESTBIN --start --shell-path $SHELLPATH:6651 --iface eth1
else
	# Checking if git is already installed. It's not shipped with CernVM by default.
	# If it's not yet installed, the script will install it as it needs it to retrieve
	# the source code of cvmfs-test.
	if [ "$GITBIN" == "" ] ; then
		conary update git
		GITBIN=`which git 2> /dev/null`
	fi
	
	# Changing current directory to root home
	cd /root
	
	# Cloning the repo ad installing the software
	$GITBIN clone git://github.com/ruvolof/cvmfs-test cvmfs-test
	./cvmfs-test/Install.pl
	CVMFSTESTBIN=`which cvmfs-test 2> /dev/null`
	
	# Starting the daemon
	$CVMFSTESTBIN --start --shell-path $SHELLPATH --iface eth1
fi

exit 0