#!/bin/bash
#
# This script will contextualize a Cern Virtual Machine.

SHELLPATH="DAEMON_WILL_EDIT_THIS_ON_EVERY_BOOT"
CPS="/root/current_shell_path.txt"
CONTEXTSH="/root/start_cvmfstestd.sh"
DOWNLOADING_SCRIPT="wget -O $CONTEXTSH https://raw.github.com/ruvolof/cvmfs-test/master/Distributed/start_cvmfstestd.sh"
RCLOCAL_DOWNLOAD=`grep "$DOWNLOADING_SCRIPT" /etc/rc.local 2>/dev/null`
RCLOCAL=`grep "$CONTEXTSH" /etc/rc.local 2>/dev/null`

start() {
	if [ "$RCLOCAL_DOWNLOAD" == "" ] ; then
		echo "wget -O $CONTEXTSH https://raw.github.com/ruvolof/cvmfs-test/master/Distributed/start_cvmfstestd.sh" >> /etc/rc.local
		echo "chmod +x $CONTEXTSH" >> /etc/rc.local
	fi

	if [ "$RCLOCAL" == "" ] ; then
		echo $CONTEXTSH >> /etc/rc.local
	fi

	echo $SHELLPATH > $CPS
}

case "$1" in
'start')
	start
	;;
*)
	echo "Usage: prolog.sh start"
esac

exit 0
