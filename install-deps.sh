#!/bin/bash
#
# Install dependencies for CernVM File System
#
# Weiwei Jia <harryxiyou@gmail.com>, March 2015

if test $(id -u) != 0 ; then
	SUDO=sudo
fi

if which apt-get > /dev/null ; then
	$SUDO apt-get install -y lsb-release
fi

case $(lsb_release -sc) in
precise)
	packages=cmake build-essential zlib1g-dev make libattr1-dev libssl-dev \
		ssl libssl python-dev python libfuse-dev libuuid1 uuid uuid-dev
    $SUDO apt-get install -y $packages
	;;
*)
	echo "Distro version unknown, dependencies will have to be installed manually."
	;;
esac


