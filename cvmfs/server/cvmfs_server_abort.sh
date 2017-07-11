#
# This file is part of the CernVM File System
# This script takes care of creating, removing, and maintaining repositories
# on a Stratum 0/1 server
#
# Implementation of the "cvmfs_server abort command"

cvmfs_server_abort() {
  local names
  local user
  local gw_key_file
  local spool_dir
  local exact=0
  local force=0
  local open_fd_dialog=1
  local retcode=0

  # optional parameter handling
  OPTIND=1
  while getopts "fe" option
  do
    case $option in
      f)
        force=1
        open_fd_dialog=0
      ;;
      e) # Need this mode if passing repository subpaths: cvmfs_server transaction myrepo.cern.ch/some/subpath
        exact=1
      ;;
      ?)
        shift $(($OPTIND-2))
        usage "Command abort: Unrecognized option: $1"
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

  for name in $names; do

    # Check if the repo name contains a subpath for locking, e.g. repo.cern.ch/sub/path/for/locking
    local subpath=$(echo $name | cut -d'/' -f2- -s)
    name=$(echo $name | cut -d'/' -f1)

    # sanity checks
    is_stratum0 $name   || { echo "Repository $name is not a stratum 0 repository"; retcode=1; continue; }
    is_publishing $name && { echo "Repository $name is currently published (aborting abort)"; retcode=1; continue; }

    # get repository information
    load_repo_config $name
    user=$CVMFS_USER
    gw_key_file=/etc/cvmfs/keys/${name}.gw
    spool_dir=$CVMFS_SPOOL_DIR
    local upstream_storage=$CVMFS_UPSTREAM_STORAGE
    local upstream_type=$(get_upstream_type $upstream_storage)

    # more sanity checks
    is_owner_or_root $name || { echo "Permission denied: Repository $name is owned by $user"; retcode=1; continue; }
    check_repository_compatibility $name
    if [ x"$upstream_type" != xgw ]; then
        is_in_transaction $name || { echo "Repository $name is not in a transaction"; retcode=1; continue; }
    fi
    is_cwd_on_path "/cvmfs/$name" && { echo "Current working directory is in /cvmfs/$name.  Please release, e.g. by 'cd \$HOME'."; retcode=1; continue; } || true

    # better ask the user once again!
    if [ $force -ne 1 ]; then
      local reply
      read -p "You are about to DISCARD ALL CHANGES OF THE CURRENT TRANSACTION for $name!  Are you sure (y/N)? " reply
      if [ "$reply" != "y" ] && [ "$reply" != "Y" ]; then
        continue
      fi
    fi

    # do a health check (might also repair out-of-sync in_transaction repos)
    health_check -rt $name

    # check if we have open file descriptors on /cvmfs/<name>
    local use_fd_fallback=0
    handle_read_only_file_descriptors_on_mount_point $name $open_fd_dialog || use_fd_fallback=1
    sync

    to_syslog_for_repo $name "aborting transaction"

    abort_before_hook $name
    # If the upstream storage type is http (publication leases are managed by an instance of the CVMFS repo services,
    # the cvmfs_swissknife lease command needs to be used to drop the active lease
    if [ x"$upstream_type" = xgw ]; then
        local repo_services_url=$(echo $upstream_storage | cut -d',' -f3)
        __swissknife lease -a drop -u $repo_services_url -k $gw_key_file -p $name"/"$subpath || { echo "Could not drop active lease or lease does not exist for repository $name"; retcode=1; continue; }
    fi
    close_transaction $name $use_fd_fallback

    abort_after_hook $name

  done

  return $retcode
}
