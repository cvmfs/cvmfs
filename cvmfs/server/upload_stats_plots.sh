#!/bin/bash

# loads the configuration for a specific repository
load_repo_config() {
  local name=$1
  . /etc/cvmfs/repositories.d/${name}/server.conf
  if ! [ x"$CVMFS_REPOSITORY_TYPE" = x"stratum0" ]; then
    . /etc/cvmfs/repositories.d/${name}/replica.conf
  fi
}

upload_statistics_plots_bg() {
  local db_path_copied="$1"
  local repo_name="$2"
  local upstream="$3"
  root_file=$(mktemp -t stats.root.XXXXXX)
  root -l -b -q /usr/share/cvmfs-server/generate_stats_plots.C\("\"${db_path_copied}\", \"${root_file}\""\)
  cvmfs_swissknife upload -i ${root_file} \
                          -o stats/stats.root \
                          -r $upstream
  
  index_file=$(mktemp -t stats_index.html.XXXXXX)
  sed "s/\$REPO_NAME/$repo_name/g" /usr/share/cvmfs-server/stats_index.html.tpl > $index_file 

  cvmfs_swissknife upload -i $index_file \
                          -o stats/index.html \
                          -r $upstream
  rm $db_path_copied
  rm $root_file
  rm $index_file
}

if [ -z "$1" ]; then
  echo "$0: missing argument: repository name"
  exit 1
fi
name="$1"
load_repo_config $name

if [ "x${CVMFS_UPLOAD_STATS_PLOTS}" != "xtrue" ]; then
  echo "CVMFS_UPLOAD_STATS_PLOTS option for this repo is disabled. No plots will be uploaded."
  exit 0
fi

if ! command -v root 1>/dev/null ; then
  echo "Requested statistics plots upload but no ROOT installed. No plots will be uploaded."
  exit 1
fi

db_path="/var/spool/cvmfs/${name}/stats.db"

db_path_copied=$(mktemp -t stats.db.XXXXXX)
cp $db_path $db_path_copied
upload_statistics_plots_bg $db_path_copied $name $CVMFS_UPSTREAM_STORAGE 1>/dev/null &
