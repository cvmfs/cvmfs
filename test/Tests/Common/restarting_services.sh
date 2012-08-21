# This script is executed to restart cvmfs service and autofs.
# Run it with option 'save_cache' option to avoid cvmfs restartclean command,
# which erases cvmfs cache.

# cvmfs startup script
CVMFS="/etc/init.d/cvmfs"

# autofs startup script
AUTOFS="/etc/init.d/autofs"

# This check is done, actually, only for slackware compatibility
if [ ! -f $AUTOFS ] ; then
	if [ -f "/etc/rc.d/rc.autofs" ] ; then
		AUTOFS="etc/rc.d/rc.autofs"
	fi
fi

$CVMFS start

if [ $# -lt 1 ] || [ $1 != "save_cache" ] ; then
	$CVMFS restartclean
fi

$AUTOFS stop
killall -9 automount
$AUTOFS start

exit 0
