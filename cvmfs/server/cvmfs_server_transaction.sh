#
# This file is part of the CernVM File System
# This script takes care of creating, removing, and maintaining repositories
# on a Stratum 0/1 server
#
# Implementation of the "cvmfs_server transaction command"

# This file depends on fuctions implemented in the following files:
# - cvmfs_server_util.sh
# - cvmfs_server_common.sh
# - cvmfs_server_health_check.sh
# - cvmfs_server_compat.sh

cvmfs_server_transaction() {
  local names
  local spool_dir
  local stratum0
  local exact=0
  local force=0
  local retcode=0

  # optional parameter handling
  OPTIND=1
  while getopts "fe" option
  do
    case $option in
      f)
        force=1
      ;;
      e) # Need this mode if passing repository subpaths: cvmfs_server transaction myrepo.cern.ch/some/subpath
        exact=1
      ;;
      ?)
        shift $(($OPTIND-2))
        usage "Command transaction: Unrecognized option: $1"
      ;;
    esac
  done

  shift $(($OPTIND-1))
  check_parameter_count_for_multiple_repositories $#
  # get repository names
  if [ $exact -eq 0 ]; then
      names=$(get_or_guess_multiple_repository_names "$@")
      check_multiple_repository_existence "$names"
  else
      names=$@
  fi

  # sanity checks
  check_autofs_on_cvmfs && die "Autofs on /cvmfs has to be disabled"

  # go through the repositories
  for name in $names; do

    # Check if the repo name contains a subpath for locking, e.g. repo.cern.ch/sub/path/for/locking
    local subpath=$(echo $name | cut -d'/' -f2- -s)
    name=$(echo $name | cut -d'/' -f1)

    # sanity checks
    is_stratum0 $name || { echo "Repository $name is not a stratum 0 repository"; retcode=1; continue; }
    health_check -r $name

    # get repository information
    load_repo_config $name
    spool_dir=$CVMFS_SPOOL_DIR
    stratum0=$CVMFS_STRATUM0
    local upstream_storage=$CVMFS_UPSTREAM_STORAGE
    local upstream_type=$(get_upstream_type $upstream_storage)
    user=$CVMFS_USER

    # more sanity checks
    is_owner_or_root $name || { echo "Permission denied: Repository $name is owned by $user"; retcode=1; continue; }
    check_repository_compatibility $name
    if [ $force -eq 0 ]; then
        is_in_transaction $name && { echo "Repository $name is already in a transaction"; retcode=1; continue; }
    fi
    check_expiry $name $stratum0 || { echo "Repository whitelist for $name is expired!"; retcode=1; continue; }
    [ $(get_expiry $name $stratum0) -le $(( 12 * 60 * 60 )) ] && { echo "Warning: Repository whitelist stays valid for less than 12 hours!"; }

    # do it!
    transaction_before_hook $name
    # If the upstream storage type is http (publication leases are managed by an instance of the CVMFS repo services,
    # the cvmfs_swissknife lease command needs to be used to acquire a new lease
    if [ x"$upstream_type" = xhttp ]; then
        local repo_services_url=$(echo $upstream_storage | cut -d',' -f3)
        __swissknife lease -a acquire -u $repo_services_url -n $user -p $name"/"$subpath || { echo "Could not acquire a new lease for repository $name"; retcode=1; continue; }
    fi
    open_transaction $name $

    transaction_after_hook  $name

  done

  return $retcode
}

