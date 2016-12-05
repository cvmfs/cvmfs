#
# This file is part of the CernVM File System
# This script takes care of creating, removing, and maintaining repositories
# on a Stratum 0/1 server
#
# Implementation of the "cvmfs_server info" command

# This file depends on fuctions implemented in the following files:
# - cvmfs_server_util.sh
# - cvmfs_server_common.sh

cvmfs_server_info() {
  local name
  local stratum0

  # get repository name
  check_parameter_count_with_guessing $#
  name=$(get_or_guess_repository_name $1)

  # sanity checks
  check_repository_existence $name || die "The repository $name does not exist"
  is_stratum0 $name || die "This is not a stratum 0 repository"

  # get repository information
  load_repo_config $name
  stratum0=$CVMFS_STRATUM0

  # do it!
  echo "Repository name: $name"
  echo "Created by CernVM-FS $(mangle_version_string $(repository_creator_version $name))"
  local replication_allowed="yes"
  is_master_replica $name || replication_allowed="no"
  echo "Stratum1 Replication Allowed: $replication_allowed"
  local expire_countdown=$(get_expiry $name $stratum0)
  if [ $expire_countdown -le 0 ]; then
    echo "Whitelist is expired"
  else
    local valid_time=$(( $expire_countdown/(3600*24) ))
    echo "Whitelist is valid for another $valid_time days"
  fi
  echo

  echo "\
Client configuration:
Add $name to CVMFS_REPOSITORIES in /etc/cvmfs/default.local
Create /etc/cvmfs/config.d/${name}.conf and set
  CVMFS_SERVER_URL=$stratum0
  CVMFS_PUBLIC_KEY=/etc/cvmfs/keys/${name}.pub
Copy /etc/cvmfs/keys/${name}.pub to the client"
}


