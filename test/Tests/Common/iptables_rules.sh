# This is script will be use to backup, restore and modify iptables rules
# to forward port during test.

IPTABLES_STARTUP="/etc/init.d/iptables"
IPTABLES="/sbin/iptables"
IPTABLES_SAVE="/sbin/iptables-save"
IPTABLES_RESTORE="/sbin/iptables-restore"

if [ ! -f $IPTABLES ] ; then
	if [ -f "/usr/sbin/iptables" ] ; then
		IPTABLES="/usr/sbin/iptables"
	fi
fi

if [ ! -f $IPTABLES_SAVE ] ; then
	if [ -f "/usr/sbin/iptables-save" ] ; then
		IPTABLES="/usr/sbin/iptables-save"
	fi
fi

if [ ! -f $IPTABLES_RESTORE ] ; then
	if [ -f "/usr/sbin/iptables-restore" ] ; then
		IPTABLES="/usr/sbin/iptables-restore"
	fi
fi

case "$1" in
	"backup")
		if [ -f $IPTABLES_STARTUP ] ; then
			$IPTABLES_STARTUP save
		else
			$IPTABLES_SAVE > saved_iptables_rules
		fi
		;;
	"restore")
		if [ -f $IPTABLES_STARTUP ] ; then
			$IPTABLES_STARTUP restart
		else
			$IPTABLES_RESTORE < saved_iptables_rules
		fi
		;;
	"forward")
		$IPTABLES -t nat -A OUTPUT -p tcp --dport $2 -j DNAT --to-destination 127.0.0.1:$3
		$IPTABLES -t nat -A OUTPUT -p udp --dport $2 -j DNAT --to-destination 127.0.0.1:$3
		;;
	*)
		echo "No action set for this argument."
esac

exit 0
