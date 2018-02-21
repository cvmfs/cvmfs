#!/bin/sh

set -e

SCRIPT_LOCATION=$(cd "$(dirname "$0")"; pwd)

action=$1

if [ x"$action" = xstart ]; then
    RUNNER_LOG_DIR=/tmp $SCRIPT_LOCATION/../bin/cvmfs_services start
    echo "CVMFS gateway services started."
elif [ x"$action" = xstop ]; then
    RUNNER_LOG_DIR=/tmp $SCRIPT_LOCATION/../bin/cvmfs_services stop
    echo "CVMFS gateway services stopped."
elif [ x"$action" = xstatus ]; then
    RUNNER_LOG_DIR=/tmp $SCRIPT_LOCATION/../bin/cvmfs_services status
else
    echo "Unknown action: $action"
    echo "Usage: run_cvmfs_services.sh <start|stop|status>"
    exit 1
fi
