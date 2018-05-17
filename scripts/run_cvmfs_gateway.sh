#!/bin/sh

set -e

export RUNNER_LOG_DIR=/var/log/cvmfs-gateway-runner

wait_for_app_start() {
    local reply=$($SCRIPT_LOCATION/../bin/cvmfs_gateway ping | awk {'print $1'})
    local num_iter=1
    while [ $reply != "pong" ]; do
        sleep 1
        reply=$(/usr/libexec/cvmfs-gateway/bin/cvmfs_gateway ping | awk {'print $1'})
        num_iter=$((num_iter + 1))
        if [ $num_iter -eq 10 ]; then
            echo "Error: Could not start cvmfs-gateway"
            exit 1
        fi
    done
    echo $reply
}

SCRIPT_LOCATION=$(cd "$(dirname "$0")"; pwd)

action=$1

if [ x"$action" = xstart ]; then
    $SCRIPT_LOCATION/../bin/cvmfs_gateway start
    wait_for_app_start
    echo "CVMFS repository gateway started."
elif [ x"$action" = xstop ]; then
    $SCRIPT_LOCATION/../bin/cvmfs_gateway stop
    pkill epmd
    echo "CVMFS repository gateway stopped."
elif [ x"$action" = xreload ]; then
    $SCRIPT_LOCATION/../bin/cvmfs_gateway escript scripts/reload_repo_config.escript
    echo "CVMFS repository gateway configuration reloaded."
elif [ x"$action" = xrestart ]; then
    $SCRIPT_LOCATION/../bin/cvmfs_gateway stop
    pkill epmd
    $SCRIPT_LOCATION/../bin/cvmfs_gateway start
    wait_for_app_start
    echo "CVMFS repository gateway restarted."
elif [ x"$action" = xstatus ]; then
    $SCRIPT_LOCATION/../bin/cvmfs_gateway ping
else
    echo "Unknown action: $action"
    echo "Usage: run_cvmfs_gateway.sh <start|stop|reload|restart|status>"
    exit 1
fi
