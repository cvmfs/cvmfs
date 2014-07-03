# Don't edit this. 
# This may be invoked from a domain.d file to automatically order the
#   servers by response time
# Expects these parameters
#  first parameter: fully qualified repository name to probe, can be any
#	one that is always existing on the servers for the desired domain
#  the rest of the parameters are a list of server names & ports
# An ordered list of URLs is sent to stdout, unless the corresponding
#	/etc/cvmfs/domain.d/<domain>.local file is going to override it;
#	in that case nothing is sent to stdout
# The order is saved in a file for the whole domain, so it does not need to
#	be regenerated on every mount of every repo, but if this file changes
#	then the order will be recalculated on the next mount
# Requires curl, and if date isn't the GNU date, also requires perl
# Written by Dave Dykstra 20 June 2014

getnanoseconds()
{
    local NANOSECS
    NANOSECS="$(date +%s%N 2>/dev/null)"
    case "$NANOSECS" in
	*"N"|"")
	    NANOSECS="$(perl -e 'use Time::HiRes qw(gettimeofday);my ($s,$us)=gettimeofday;printf "%d%06d000\n", $s, $us;')"
	    ;;
    esac
    echo "$NANOSECS"
}

orderservers()
{
    # determine the best order for the default servers, keeping the 
    #  order in a file
    local PROBE_REPO DOMAIN LOCAL_FILE
    PROBE_REPO="$1"
    shift
    DOMAIN="${PROBE_REPO#*\.}"
    LOCAL_FILE="/etc/cvmfs/domain.d/${DOMAIN}.local"
    if [ -f "$LOCAL_FILE" ] && grep -q "^CVMFS_SERVER_URL=" $LOCAL_FILE; then
	# no need to bother to figure out ordering if local config
	#  is going to immediately override it
	return
    fi

    local ORDER_FILE SUBPATH PROBE_PATH THIS_FILE ORDERED_SERVERS SERVER
    ORDER_FILE=/etc/cvmfs/domain.d/${DOMAIN}.serverorder
    SUBPATH="cvmfs/@fqrn@"
    THIS_FILE=/etc/cvmfs/serverorder.sh

    if [ -f $ORDER_FILE ] && 
    		[ -n "`find $THIS_FILE -cnewer $ORDER_FILE`" ]; then
	# this file has changed, regenerate the order file
	# redirect errors to /dev/null because after rpm upgrade this
	#  can run as unprivileged user from cvmfs_config
	rm -f $ORDER_FILE 2>/dev/null
    fi

    if [ -s $ORDER_FILE ]; then
	source $ORDER_FILE
    else
	local PROBE_PATH
	PROBE_PATH="${SUBPATH/@fqrn@/$PROBE_REPO}/.cvmfspublished"
	ORDERED_SERVERS="$(S=0; for SERVER in "$@"; do
		# for each, take the fastest of 2 attempts, because of caching
		# if all attempts fail, leave server order unchanged
		let S+=1
		let FASTEST=99999999999+$S
		for N in 1 2; do
		    START="$(getnanoseconds)"
		    if curl -s --fail --max-time 2 --proxy "" http://$SERVER/$PROBE_PATH >/dev/null; then
			ELAPSED="$(($(getnanoseconds)-START))"
			if [ "$ELAPSED" -lt "$FASTEST" ]; then
			    FASTEST="$ELAPSED"
			fi
		    fi
		done
		echo "$FASTEST $SERVER"
	    done | sort -n | sed 's/.* //')"
	echo ORDERED_SERVERS=\"$ORDERED_SERVERS\" >$ORDER_FILE
    fi

    local SERVER SERVER_URLS
    SERVER_URLS=""
    for SERVER in $ORDERED_SERVERS; do
	SERVER="http://$SERVER/$SUBPATH"
	if [ -z "$SERVER_URLS" ]; then
	    SERVER_URLS="$SERVER"
	else
	    SERVER_URLS="$SERVER_URLS;$SERVER"
	fi
    done
    echo "$SERVER_URLS"
}
