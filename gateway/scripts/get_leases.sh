#!/bin/sh

SCRIPT_LOCATION=$(cd "$(dirname "$0")"; pwd)

RUNNER_LOG_DIR=/tmp $SCRIPT_LOCATION/../bin/cvmfs_services escript scripts/get_leases.escript
