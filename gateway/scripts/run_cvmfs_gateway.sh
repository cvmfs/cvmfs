#!/bin/sh

set -e

SCRIPT_LOCATION=$(cd "$(dirname "$0")"; pwd)

action=$1

if [ x"$action" = xstart ]; then
    RUNNER_LOG_DIR=/tmp $SCRIPT_LOCATION/../bin/cvmfs_gateway start
    echo "CVMFS repository gateway started."
elif [ x"$action" = xstop ]; then
    RUNNER_LOG_DIR=/tmp $SCRIPT_LOCATION/../bin/cvmfs_gateway stop
    echo "CVMFS repository gateway stopped."
elif [ x"$action" = xstatus ]; then
    RUNNER_LOG_DIR=/tmp $SCRIPT_LOCATION/../bin/cvmfs_gateway status
else
    echo "Unknown action: $action"
    echo "Usage: run_cvmfs_gateway.sh <start|stop|status>"
    exit 1
fi
