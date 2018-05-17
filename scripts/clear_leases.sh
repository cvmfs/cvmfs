#!/bin/sh

SCRIPT_LOCATION=$(cd "$(dirname "$0")"; pwd)

RUNNER_LOG_DIR=/var/log/cvmfs-gateway-runner $SCRIPT_LOCATION/../bin/cvmfs_gateway escript scripts/clear_leases.escript
