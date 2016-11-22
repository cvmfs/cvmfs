#
# This file is part of the CernVM File System
# This script takes care of creating, removing, and maintaining repositories
# on a Stratum 0/1 server
#
# Implementation of functionality common to all "cvmfs_server" commands
#


# This file depends on fuctions implemented in the following files:
# - cvmfs_server_util.sh


# checks the parameter count for a situation where we might be able to guess
# the repository name based on the repositories present in the system
# Note: if the parameter count does not fit or if guessing is impossible,
#       this will print the usage string with an error message and exit
# Note: this method is commonly used right before invoking
#       `get_or_guess_repository_name` to check its preconditions and report
#       error before actually doing something wrong
#
# @param provided_parameter_count  number of parameters provided by the user
# @param allow_multiple_names      switches off the usage print for too many
#                                  detected script parameters (see next fn)
check_parameter_count_with_guessing() {
  local provided_parameter_count=$1
  local allow_multiple_names=$2

  if [ $provided_parameter_count -lt 1 ]; then
    # check if we have not _exactly_ one repository present
    if [ $(ls /etc/cvmfs/repositories.d/ | wc -l) -ne 1 ]; then
      usage "Please provide a repository name"
    fi
  fi

  if [ $provided_parameter_count -gt 1 ] && \
     [ x"$allow_multiple_names" = x"" ]; then
    usage "Too many arguments provided"
  fi

  return 0
}


# checks the parameter count when we accept more than one repository for the
# command.
# Note: this method prints an error message if appropriate and exists the script
#       execution
#
# @param provided_parameter_count  number of parameters provided by the user
check_parameter_count_for_multiple_repositories() {
  local provided_parameter_count=$1
  check_parameter_count_with_guessing $provided_parameter_count allow_multiple
  return $?
}


# guesses a list of repository names based on file system wildcards
#
# @param ...    all repository hints provided by the user of the script
#               Like: test.local repo.* *.cern.ch
get_or_guess_multiple_repository_names() {
  local repo_dir="/etc/cvmfs/repositories.d"
  local repo_names=""

  if [ $# -eq 0 ]; then
    repo_names=$(get_or_guess_repository_name)
    echo $repo_names
    return 0;
  fi

  for input_pattern in $@; do
    local names="$(ls --directory $repo_dir/$input_pattern 2>/dev/null)"
    if [ x"$names" = x"" ]; then
      repo_names="$repo_names $input_pattern"
    else
      for name in $names; do
        if ! contains "$repo_names" $(basename $name); then
          repo_names="$(basename $name) $repo_names"
        fi
      done
    fi
  done

  echo "$repo_names"
}


# checks if the given repository name already exists
#
# @param given_name   the name of the repository to be checked
# @return             0 if the repository was found
check_repository_existence() {
  local given_name="$1"
  local fqrn

  # empty name or wildcards are not allowed (and thus does not exist)
  if [ x"$given_name" = x ] || echo "$given_name" | grep -q "*"; then
    return 1
  fi

  # Check if exists
  fqrn=$(cvmfs_mkfqrn $given_name)
  [ -d /etc/cvmfs/repositories.d/$fqrn ]
}


# checks the existence of a list of repositories
# Note: the function echo's an error message and stops the execution of the
#       script by default.
#
# @param given_names   the list of repository names to be checked
# @param no_kill       (optional) skip the termination on error
# @return              0 if all listed repositories exist
check_multiple_repository_existence() {
  local given_names="$1"
  local no_kill=$2

  for name in $given_names; do
    if ! check_repository_existence $name; then
      if [ x"$no_kill" = x"" ]; then
        die "The repository $name does not exist."
      else
        return 1
      fi
    fi
  done
  return 0
}


# mangles the repository name into a fully qualified repository name
#
# @param repository_name       the repository name given by the user
# @return                      echoes the correct repository name to use
get_repository_name() {
  local repository_name=$1
  echo $(cvmfs_mkfqrn $repository_name)
}


# loads the configuration for a specific repository
load_repo_config() {
  local name=$1
  . /etc/cvmfs/repositories.d/${name}/server.conf
  if [ x"$CVMFS_REPOSITORY_TYPE" = x"stratum0" ]; then
    . /etc/cvmfs/repositories.d/${name}/client.conf
  else
    . /etc/cvmfs/repositories.d/${name}/replica.conf
  fi
}


# retrieves the currently mounted root catalog hash in the spool area
#
# @param repository_name    the name of the repository to be checked
# @return                   echoes the currently mounted root catalog hash
get_mounted_root_hash() {
  local repository_name=$1

  load_repo_config $repository_name
  get_repo_info -u ${CVMFS_SPOOL_DIR}/rdonly -C
}


# retrieves the latest published root catalog hash in the backend storage
#
# @param repository_name   the name of the repository to be checked
# @return                  echoes the last published (HEAD) root catalog hash
get_published_root_hash() {
  local repository_name=$1

  load_repo_config $repository_name
  get_repo_info -c
}


set_ro_root_hash() {
  local name=$1
  local root_hash=$2
  local client_config=/var/spool/cvmfs/${name}/client.local

  if grep -q ^CVMFS_ROOT_HASH= ${client_config}; then
    sed -i -e "s/CVMFS_ROOT_HASH=.*/CVMFS_ROOT_HASH=${root_hash}/" $client_config
  else
    echo "CVMFS_ROOT_HASH=${root_hash}" >> $client_config
  fi
}


get_repo_info_from_url() {
  local url="$1"
  shift 1
  __swissknife info $(get_follow_http_redirects_flag) -r "$url" $@ 2>/dev/null
}


# expects load_repo_config to be called with the right repository before
# possible parameters, see `cvmfs_swissknife info`
get_repo_info() {
  get_repo_info_from_url "$CVMFS_STRATUM0" $@
}


# checks if a repository is currently in a transaction
#
# @param name  the repository name to be checked
# @return      0 if in a transaction
is_in_transaction() {
  local name=$1
  load_repo_config $name
  check_lock ${CVMFS_SPOOL_DIR}/in_transaction ignore_stale
}


# checks if a repository is currently runing a publish procedure
#
# @param name  the repository name to be checked
# @return      0 if a publishing procedure is running
is_publishing() {
  local name=$1
  load_repo_config $name
  check_lock ${CVMFS_SPOOL_DIR}/is_publishing ignore_stale
}


# checks if the running user is root
#
# @return   0 if the current user is root
is_root() {
  [ $(id -u) -eq 0 ]
}


# checks if the running user is either the owner of a repository or root
#
# @param name  the name of the repository
is_owner_or_root() {
  local name="$1"
  is_root && return 0
  load_repo_config $name
  [ x"$(whoami)" = x"$CVMFS_USER" ]
}


# download a given file from the backend storage
# @param noproxy  (optional)
get_item() {
  local name="$1"
  local url="$2"
  local noproxy="$3"

  load_repo_config $name

  if [ x"$noproxy" != x"" ]; then
    unset http_proxy && curl $(get_follow_http_redirects_flag) "$url" 2>/dev/null
  else
    curl $(get_follow_http_redirects_flag) "$url" 2>/dev/null
  fi
}


# Parse redirects configuration into redirect flag.
# Assumes that config is loaded already, i.e. load_repo_config().
get_follow_http_redirects_flag() {
  if [ x"$CVMFS_FOLLOW_REDIRECTS" = x"yes" ]; then
    echo "-L"
  fi
}


get_expiry_from_string() {
  local whitelist="$1"

  local expires=$(echo "$whitelist" | grep --text -e '^E[0-9]\{14\}$' | tr -d E)
  if echo "$expires" | grep -q -E --invert-match '^[0-9]{14}$'; then
    echo -1
    return 1
  fi
  local year=$(echo $expires | head -c4)
  local month=$(echo $expires | head -c6 | tail -c2)
  local day=$(echo $expires | head -c8 | tail -c2)
  local hour=$(echo $expires | head -c10 | tail -c2)
  local minute=$(echo $expires | head -c12 | tail -c2)
  local second=$(echo $expires | head -c14 | tail -c2)
  local expires_fmt="${year}-${month}-${day} ${hour}:${minute}:${second}"
  local expires_num=$(date -u -d "$expires_fmt" +%s)

  local now=$(/bin/date -u +%s)
  local valid_countdown=$(( $expires_num-$now ))
  echo $valid_countdown
}


# figures out the time to expiry of the repository's whitelist
#
# @param stratum0  path/URL to stratum0 storage
# @return          number of seconds until expiry (negativ if already expired)
get_expiry() {
  local name=$1
  local stratum0=$2
  get_expiry_from_string "$(get_item $name $stratum0/.cvmfswhitelist 'noproxy')"
}


# checks if the repository's whitelist is valid
#
# @param stratum0  path/URL to stratum0 storage
# @return          0 if whitelist is still valid
check_expiry() {
  local name=$1
  local stratum0=$2
  local expiry="-1"

  expiry=$(get_expiry $name $stratum0)
  if [ $? -ne 0 ]; then
    echo "Failed to retrieve repository expiry date" >&2
    return 100
  fi

  [ $expiry -ge 0 ]
}


open_transaction() {
  local name=$1
  load_repo_config $name
  local tx_lock="${CVMFS_SPOOL_DIR}/in_transaction"

  is_stratum0 $name                    || die "Cannot open transaction on Stratum1"
  acquire_lock "$tx_lock" ignore_stale || die "Failed to create transaction lock"
  run_suid_helper open $name           || die "Failed to make /cvmfs/$name writable"

  to_syslog_for_repo $name "opened transaction"
}

