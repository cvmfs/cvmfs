#!/bin/bash
#
# This script will contextualize a Cern Virtual Machine.

SHELLPATH="TO_EDIT"
CPS="/root/current_shell_path.txt"
CONTEXTSH="/root/start_cvmfstestd.sh"
RCLOCAL=`grep context.sh /etc/rc.local`

start() {
	if [ ! -f $CONTEXTSH ] ; then
		echo "wget -O $CONTEXTSH https://raw.github.com/ruvolof/cvmfs-test/master/Distributed/start_cvmfstestd.sh" >> /etc/rc.local
		chmod +x $CONTEXTSH
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
