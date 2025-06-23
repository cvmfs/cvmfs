#!/bin/bash

CACHE_TYPE=$1
BENCHMARK=$2

# check for dropping HOT kernel caches
if [ $CACHE_TYPE == "cold" ] || [ $CACHE_TYPE == "warm" ]; then
    cvmfs_config umount > /dev/null
fi

# drop WARM cvmfs caches
if [ $CACHE_TYPE == "cold" ]; then
    sudo rm -r /var/lib/cvmfs/shared 2>/dev/null
fi

# run benchmark
time $BENCHMARK >/dev/null 2>&1
