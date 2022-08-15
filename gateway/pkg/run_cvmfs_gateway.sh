#!/bin/sh

set -x

SCRIPT_LOCATION=$(cd "$(dirname "$0")"; pwd)

has_systemd=false
if [ x"$(pidof systemd >/dev/null && echo yes || echo no)" = "xyes" ]; then
    has_systemd=true
fi

action=$1
user=$2

systemd_service="cvmfs-gateway"
if [ "x$user" != "x" ] && [ "x$user" != "xroot" ]; then
    systemd_service="cvmfs-gateway@$user"
fi

if $has_systemd ; then
    gateway_cmd() {
        systemctl $1 $systemd_service
    }
else
    gateway_cmd() {
        service cvmfs-gateway $1
    }
fi

case $action in
"start" )
    gateway_cmd "start"
    echo "CVMFS repository gateway started."
    ;;
"stop" )
    gateway_cmd "stop"
    echo "CVMFS repository gateway stopped."
    ;;
"restart" )
    gateway_cmd "restart"
    echo "CVMFS repository gateway restarted."
    ;;
"status" )
    gateway_cmd "status" > /dev/null 2>&1
    retval=$?
    if [ "x$retval" = "x0" ]; then
        echo "running"
    else
        echo "stopped"
    fi
    ;;
"*" )
    echo "Unknown action: $action"
    echo "Usage: run_cvmfs_gateway.sh <start|stop|restart|status> [USER]"
    ;;
esac
