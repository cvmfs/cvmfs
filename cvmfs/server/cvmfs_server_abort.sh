cvmfs_server_abort() {
  local names
  local user
  local spool_dir
  local force=0
  local open_fd_dialog=1
  local retcode=0

  # optional parameter handling
  OPTIND=1
  while getopts "f" option
  do
    case $option in
      f)
        force=1
        open_fd_dialog=0
      ;;
      ?)
        shift $(($OPTIND-2))
        usage "Command abort: Unrecognized option: $1"
      ;;
    esac
  done

  # get repository names
  shift $(($OPTIND-1))
  check_parameter_count_for_multiple_repositories $#
  names=$(get_or_guess_multiple_repository_names "$@")
  check_multiple_repository_existence "$names"

  for name in $names; do

    # sanity checks
    is_stratum0 $name   || { echo "Repository $name is not a stratum 0 repository"; retcode=1; continue; }
    is_publishing $name && { echo "Repository $name is currently published (aborting abort)"; retcode=1; continue; }

    # get repository information
    load_repo_config $name
    user=$CVMFS_USER
    spool_dir=$CVMFS_SPOOL_DIR
    upstream_storage=$CVMFS_UPSTREAM_STORAGE
    upstream_type=$(get_upstream_type $upstream_storage)

    # more sanity checks
    is_owner_or_root $name || { echo "Permission denied: Repository $name is owned by $user"; retcode=1; continue; }
    check_repository_compatibility $name
    is_in_transaction $name || { echo "Repository $name is not in a transaction"; retcode=1; continue; }
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
    if [ x"$upstream_type" = xhttp ]; then
        repo_services_url=$(echo $upstream_storage | cut -d',' -f3)
        __swissknife lease -a drop -u $repo_services_url -n $user -p $name || { echo "Could not drop active lease or lease does not exist for repository $name"; retcode=1; continue; }
    fi
    close_transaction $name $use_fd_fallback

    abort_after_hook $name

  done

  return $retcode
}
