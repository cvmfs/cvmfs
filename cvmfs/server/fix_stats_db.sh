#!/bin/bash

# Fixes stats.db files where start_time is missing due to previous publisher
# statistics handling errors in the cvmfs_receiver process

# loads the configuration for a specific repository
load_repo_config() {
  local name=$1
  . /etc/cvmfs/repositories.d/${name}/server.conf
  if ! [ x"$CVMFS_REPOSITORY_TYPE" = x"stratum0" ]; then
    . /etc/cvmfs/repositories.d/${name}/replica.conf
  fi
}

if [ -z "$1" ]; then
  echo "$0: missing argument: repository name"
  exit 1
fi
name="$1"
load_repo_config $name

db_path="/var/spool/cvmfs/${name}/stats.db"

sqlite3 $db_path "update publish_statistics set start_time = finish_time where start_time = ''"

cvmfs_swissknife upload -i $db_path \
                        -o stats/stats.db \
                        -r $CVMFS_UPSTREAM_STORAGE
