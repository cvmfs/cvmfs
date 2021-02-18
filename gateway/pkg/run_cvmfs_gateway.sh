#!/bin/sh

set -e

SCRIPT_LOCATION=$(cd "$(dirname "$0")"; pwd)

has_systemd=false
if [ x"$(pidof systemd >/dev/null && echo yes || echo no)" = "xyes" ]; then
    has_systemd=true
fi

action=$1

user=$2
if [ "x$user" = "x" ]; then
    user=root
fi

if [ x"$action" = xstart ]; then
    if $has_systemd; then
        systemctl start cvmfs-gateway@$user
    else
        service cvmfs-gateway start
    fi
    echo "CVMFS repository gateway started."
elif [ x"$action" = xstop ]; then
    if $has_systemd; then
        systemctl stop cvmfs-gateway@$user
    else
        service cvmfs-gateway stop
    fi
    echo "CVMFS repository gateway started."
elif [ x"$action" = xreload ]; then
    echo "Reloading the CVMFS repository gateway is not implemented. Please use restart."
elif [ x"$action" = xrestart ]; then
    if $has_systemd; then
        systemctl restart cvmfs-gateway@$user
    else
        service cvmfs-gateway restart
    fi
    echo "CVMFS repository gateway restarted."
elif [ x"$action" = xstatus ]; then
    if $has_systemd; then
        systemctl status cvmfs-gateway@$user > /dev/null 2>&1
    else
        service cvmfs-gateway status > /dev/null 2>&1
    fi
    retval=$?
    if [ "x$retval" = "x0" ]; then
        echo "pong"
    else
        echo "CVMFS repository gateway is not running"
    fi
else
    echo "Unknown action: $action"
    echo "Usage: run_cvmfs_gateway.sh <start|stop|reload|restart|status> [USER]"
    exit 1
fi