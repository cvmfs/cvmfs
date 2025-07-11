#
# This file is part of the CernVM File System
# This script takes care of creating, removing, and maintaining repositories
# on a Stratum 0/1 server
#
# Implementation of the "cvmfs_server gc" command

# This file depends on functions implemented in the following files:
# - cvmfs_server_util.sh
# - cvmfs_server_common.sh


cvmfs_server_gc() {
  local names
  local list_deleted_objects=0
  local dry_run=0
  local preserve_revisions=-1
  local preserve_timestamp=0
  local timestamp_threshold=""
  local force=0
  local all_collect=0
  local all_collected=0
  local deletion_log=""
  local reconstruct_reflog="0"

  # optional parameter handling
  OPTIND=1
  while getopts "ldr:t:faAL:" option
  do
    case $option in
      l)
        list_deleted_objects=1
      ;;
      d)
        dry_run=1
      ;;
      r)
        preserve_revisions="$OPTARG"
      ;;
      t)
        timestamp_threshold="$OPTARG"
      ;;
      f)
        force=1
      ;;
      a)
        all_collect=1
        all_collected=1
      ;;
      A)
        all_collect=1
      ;;
      L)
        deletion_log="$OPTARG"
      ;;
      ?)
        shift $(($OPTIND-2))
        usage "Command gc: Unrecognized option: $1"
      ;;
    esac
  done
  shift $(($OPTIND-1))

  # get repository names
  if [ $all_collect -ne 0 ] && [ -z "$@" ]; then
    set -- '*'
  fi
  check_parameter_count_for_multiple_repositories $#
  names=$(get_or_guess_multiple_repository_names "$@")
  check_multiple_repository_existence "$names"

  local gclog=/var/log/cvmfs/gc.log

  if [ $all_collect -ne 0 ]; then
    # reduce the names to those that are collectable
    local collectable_names
    for name in $names; do
      if ! is_inactive_replica $name && is_garbage_collectable $name; then
        collectable_names="$collectable_names $name"
      fi
    done
    # the echo gets rid of the leading blank
    names="`echo $collectable_names`"
    if [ -z "$names" ]; then
      die "There are no active garbage-collectable repositories"
    fi
    if [ $all_collected -ne 0 ]; then
      # further reduce the list to either stratum0s or replicas that have
      # been collected upstream after they were collected here
      collectable_names=""
      for name in $names; do
        if is_stratum0 $name || __was_garbage_collected_upstream $name; then
          collectable_names="$collectable_names $name"
        elif [ $dry_run -ne 0 ]; then
          # pretend that gc was done to keep monitors happy
          update_repo_status $name last_gc "`date --utc`"
        fi
      done
      names="`echo $collectable_names`"
      if [ -z "$names" ]; then
        echo "At `date` there are no garbage-collectable repositories that were collected upstream" >> $gclog
        exit
      fi
    fi
  fi

  # parse timestamp (if given)
  if [ ! -z "$timestamp_threshold"  ]; then
    preserve_timestamp="$(date --date "$timestamp_threshold" +%s 2>/dev/null)" || die "Cannot parse time stamp '$timestamp_threshold'"
  fi

  [ $preserve_revisions -ge 0 ] && [ $preserve_timestamp -gt 0 ] && die "Please specify either timestamp OR revision thresholds (-r and -t are mutual exclusive)"
  if [ $preserve_revisions -lt 0 ] && [ $preserve_timestamp -le 0 ]; then
    # neither revision nor timestamp threshold given... fallback to default
    preserve_timestamp="$(date --date '3 days ago' +%s 2>/dev/null)"
  fi

  for name in $names; do
    if ! has_reference_log $name; then
      reconstruct_reflog=1
    fi
  done

  # TODO: Once the gateway administration endpoint is in place (CVM-1685), it should be forbidden
  #       to run GC directly on the gateway
  # Check if the command is called on a repository gateway, and if so,
  # abort if there are any active leases
  if [ -x "/usr/libexec/cvmfs-gateway/scripts/get_leases.sh" ]; then
    for name in $names; do
      if [ x"$( /usr/libexec/cvmfs-gateway/scripts/get_leases.sh | grep $name )" != x"" ]; then
        echo "Active lease found for repository: $name. Aborting"
        return 1
      fi
    done
    # If cvmfs-gateway is running, turn it off for the duration of the GC
    if [ "x$(sudo /usr/libexec/cvmfs-gateway/scripts/run_cvmfs_gateway.sh status)" = "xpong" ]; then
      echo "Turning off cvmfs-gateway"
      if is_systemd; then
        sudo systemctl stop cvmfs-gateway
      else
        sudo service cvmfs-gateway stop
      fi
      trap __restore_cvmfs_gateway EXIT HUP INT TERM
    fi
  fi



  # sanity checks
  if [ $dry_run -ne 0 ] && [ $reconstruct_reflog -ne 0 ]; then
    die "Reflog reconstruction needed. Cannot do a dry-run."
  fi

  # safety user confirmation
  if [ $force -eq 0 ] && [ $dry_run -eq 0 ]; then
    echo "YOU ARE ABOUT TO DELETE DATA! Are you sure you want to do the following:"
  fi

  if [ $force -eq 0 ] || [ $all_collect -eq 0 ]; then
    local dry_run_msg="no"
    if [ $dry_run -eq 1 ]; then dry_run_msg="yes"; fi

    local reflog_reconstruct_msg="no"
    if [ $reconstruct_reflog -eq 1 ]; then reflog_reconstruct_msg="yes"; fi

    echo "Affected Repositories:         $names"
    echo "Dry Run (no actual deletion):  $dry_run_msg"
    echo "Needs Reflog reconstruction:   $reflog_reconstruct_msg"
    if [ $preserve_revisions -ge 0 ]; then
      echo "Preserved Legacy Revisions:    $preserve_revisions"
    fi
    if [ $preserve_timestamp -gt 0 ]; then
      echo "Preserve Revisions newer than: $(date -d@$preserve_timestamp +'%Y/%m/%d %H:%M:%S')"
    fi
    if [ $preserve_revisions -le 0 ] && [ $preserve_timestamp -le 0 ]; then
      echo "Only the latest revision will be preserved."
    fi
  fi

  if [ $force -eq 0 ]; then
    echo ""
    read -p "Please confirm this action (y/N)? " reply
    if [ "$reply" != "y" ] && [ "$reply" != "Y" ]; then
      return 1
    fi
  fi

  # Use /dev/shm for the lock file because it is world-writable and goes
  # away during reboot
  local gc_all_lock=/dev/shm/cvmfs_is_gcing_all
  if [ $all_collectable -ne 0 ]; then
    if ! acquire_lock $gc_all_lock; then
      to_syslog "skipping starting cvmfs_server gc -a because $gc_all_lock held by active process"
      return 1
    fi
  fi

  for name in $names; do

    if [ $all_collect -eq 0 ]; then
      __do_gc_cmd "$name"                       \
                  "$dry_run"                    \
                  "$list_deleted_objects"       \
                  "$preserve_revisions"         \
                  "$preserve_timestamp"         \
                  "$deletion_log"
    else
      (
      echo
      echo "Starting $name at `date`"
      # Work around the errexit (that is, set -e) misfeature of being
      #  disabled whenever the exit code is to be checked.
      # See https://lists.gnu.org/archive/html/bug-bash/2012-12/msg00093.html
      set +e
      (set -e
      __do_gc_cmd "$name"                       \
                  "$dry_run"                    \
                  "$list_deleted_objects"       \
                  "$preserve_revisions"         \
                  "$preserve_timestamp"         \
                  "$deletion_log"
      )
      if [ $? != 0 ]; then
        echo "ERROR from cvmfs_server gc!" >&2
      fi
      echo "Finished $name at `date`"
      ) >> $gclog 2>&1

      # Always return success because this is used from cron and we
      #  don't want cron sending an email every time something fails.
      # Errors will be in the log.
    fi
  done

  if [ $all_collectable -ne 0 ]; then
    release_lock $gc_all_lock
  fi
}

__restore_cvmfs_gateway() {
  echo "Restoring cvmfs-gateway"
  if is_systemd; then
    sudo systemctl start cvmfs-gateway
  else
    sudo service cvmfs-gateway start
  fi
}

# return true (0) if the upstream repo was garbage collected more recently
# than the local repo, otherwise return a positive number indicating which
# of the various ways it was not found to be more recent
__was_garbage_collected_upstream() {
  local name="$1"

  load_repo_config $name
  local upstreamstatus="$(get_url "${CVMFS_STRATUM0}/.cvmfs_status.json" 10 2>/dev/null)"
  if [ -z "$upstreamstatus" ]; then
    # status file not found upstream
    return 1
  fi
  local upstreamdate="$(get_json_field "$upstreamstatus" "last_gc")"
  if [ -z "$upstreamdate" ]; then
    # last_gc date not found in status file upstream
    return 2
  fi
  local localstatus="$(read_repo_item $name .cvmfs_status.json)"
  if [ -z "$localstatus" ]; then
    # status file not found locally
    return 0
  fi
  local localdate="$(get_json_field "$localstatus" "last_gc")"
  if [ -z "$localdate" ]; then
    # last_gc date not found locally
    return 0
  fi
  local upstreamepoch="$(date --date="$upstreamdate" +%s)"
  if [ -z "$upstreamepoch" ]; then
    # couldn't convert upstream last_gc date to epoch time
    return 3
  fi
  local localepoch="$(date --date="$localdate" +%s)"
  if [ -z "$localepoch" ]; then
    # couldn't convert local last_gc date to epoch time
    return 0
  fi
  if [ "$localepoch" -gt "$upstreamepoch" ]; then
    # local last_gc time is greater than upstream last_gc time
    return 4
  fi
  # local last_gc time is less than (or equal to) upstream last_gc time
  return 0
}

# this is used when gc is invoked from the cvmfs_server command line
__do_gc_cmd()
{
  local name="$1"
  local dry_run="$2"
  local list_deleted_objects="$3"
  local preserve_revisions="$4"
  local preserve_timestamp="$5"
  local deletion_log="$6"

  # leave extra layer of indent for now to better show diff with previous

    CVMFS_PASSTHROUGH=false
    load_repo_config $name

    # sanity checks
    check_repository_compatibility $name
    check_url "${CVMFS_STRATUM0}/.cvmfspublished" 20 || die "Repository unavailable under $CVMFS_STRATUM0"
    if [ x"$CVMFS_PASSTHROUGH" = x"true" ]; then
      echo "Repository $name is a pass-through repository, nothing to do"
      return 0
    fi
    if is_empty_repository $name; then
      echo "Repository $name is empty, nothing to do"
      return 0
    fi
    is_garbage_collectable $name || die "Garbage Collection is not enabled for $name"
    is_owner_or_root       $name || die "Permission denied: Repository $name is owned by $user"

    # figure out the URL of the repository
    local repository_url="$CVMFS_STRATUM0"
    if is_stratum1 $name; then
      [ ! -z $CVMFS_STRATUM1 ] || die "Missing CVMFS_STRATUM1 URL in server.conf"
      repository_url="$CVMFS_STRATUM1"
    fi

    # generate the garbage collection configuration
    local additional_switches="${CVMFS_SERVER_FLAGS}"
    [ $list_deleted_objects -ne 0 ] && additional_switches="$additional_switches -l"
    [ $dry_run              -ne 0 ] && additional_switches="$additional_switches -d"
    [ $preserve_revisions   -ge 0 ] && additional_switches="$additional_switches -h $preserve_revisions"
    [ $preserve_timestamp   -gt 0 ] && additional_switches="$additional_switches -z $preserve_timestamp"

    local trapcmd
    if [ $dry_run -eq 0 ]; then
      # if a check or other gc is in progress on this repo, abort
      acquire_gc_lock $name gc 1 || die "Failed to acquire gc lock for $name"
      trapcmd="release_gc_lock $name"
      trap "$trapcmd" EXIT HUP INT TERM
      if is_stratum0 $name; then
        is_in_transaction $name  && die "Cannot run garbage collection while in a transaction"
        trapcmd="close_transaction $name 0; $trapcmd"
        trap "$trapcmd" EXIT HUP INT TERM
        open_transaction $name   || die "Failed to open transaction for garbage collection"
      else
        # on stratum1
        # if an update is in progress, wait for it
        acquire_update_lock $name gc || die "Failed to acquire update lock for $name"
        trapcmd="release_update_lock $name; $trapcmd"
        trap "$trapcmd" EXIT HUP INT TERM
      fi
    fi

    local reconstruct_this_reflog=0
    if ! has_reference_log $name; then
      reconstruct_this_reflog=1
    fi

    # run the garbage collection
    local reflog_reconstruct_msg=""
    [ $reconstruct_this_reflog -ne 0 ] && reflog_reconstruct_msg="(reconstructing reference logs)"
    echo "Running Garbage Collection $reflog_reconstruct_msg"
    __run_gc "$name"                    \
             "$repository_url"          \
             "$dry_run"                 \
             "$deletion_log"            \
             "$reconstruct_this_reflog" \
             $additional_switches || die "Fail ($?)!"

    if [ $dry_run -eq 0 ]; then
      if is_stratum0 $name; then
        if [ "x$CVMFS_UPLOAD_STATS_PLOTS" = "xtrue" ]; then
          /usr/share/cvmfs-server/upload_stats_plots.sh $name
        fi
      fi
      # release lock(s) and close transaction
      eval $trapcmd
      trap - EXIT HUP INT TERM
    fi

    syncfs cautious
}

# this is used for both auto-gc (after publish or snapshot) and non-auto-gc
#   (when invoked from the cvmfs_server command line)
__run_gc() {
  local name="$1"
  local repository_url="$2"
  local dry_run="$3"
  local deletion_log="$4"
  local reconstruct_reflog="$5"
  shift 5
  local additional_switches="$*"

  load_repo_config $name

  # sanity checks
  is_garbage_collectable $name  || return 1
  [ x"$repository_url" != x"" ] || return 2
  if [ $dry_run -eq 0 ]; then
    is_in_transaction $name || is_stratum1 $name || return 3
  else
    [ $reconstruct_reflog -eq 0 ] || return 8
  fi

  if ! has_reference_log $name && [ $reconstruct_reflog -eq 0 ]; then
    return 9
  fi

  # handle a configured deletion log (manually passed log has precedence)
  if [ x"$deletion_log" != x"" ]; then
    additional_switches="$additional_switches -L $deletion_log"
  elif [ ! -z $CVMFS_GC_DELETION_LOG ]; then
    additional_switches="$additional_switches -L $CVMFS_GC_DELETION_LOG"
  fi

  if [ x"$CVMFS_UPLOAD_STATS_DB" = x"true" ]; then
    additional_switches="$additional_switches -I"
  fi

  # do it!
  local user_shell="$(get_user_shell $name)"

  if [ $reconstruct_reflog -ne 0 ]; then
    to_syslog_for_repo $name "reference log reconstruction started"
    local reflog_reconstruct_command="$(__swissknife_cmd dbg) reconstruct_reflog \
                                                  -r $repository_url             \
                                                  $(get_swissknife_proxy)        \
                                                  -u $CVMFS_UPSTREAM_STORAGE     \
                                                  -n $CVMFS_REPOSITORY_NAME      \
                                                  -t ${CVMFS_SPOOL_DIR}/tmp/     \
                                                  -k $CVMFS_PUBLIC_KEY           \
                                                  -R $(get_reflog_checksum $name)"
    if ! $user_shell "$reflog_reconstruct_command"; then
      to_syslog_for_repo $name "failed to reconstruction reference log"
    else
      to_syslog_for_repo $name "successfully reconstructed reference log"
    fi
  fi

  [ $dry_run -ne 0 ] || to_syslog_for_repo $name "started garbage collection"
  local gc_command="$(__swissknife_cmd dbg) gc                              \
                                            -r $repository_url              \
                                            $(get_swissknife_proxy)         \
                                            -u $CVMFS_UPSTREAM_STORAGE      \
                                            -n $CVMFS_REPOSITORY_NAME       \
                                            -k $CVMFS_PUBLIC_KEY            \
                                            -t ${CVMFS_SPOOL_DIR}/tmp/      \
                                            -R $(get_reflog_checksum $name) \
                                            $additional_switches"

  if ! $user_shell "$gc_command"; then
    [ $dry_run -ne 0 ] || to_syslog_for_repo $name "failed to garbage collect"
    return 6
  fi

  [ $dry_run -ne 0 ] || update_repo_status $name last_gc "`date --utc`"
  [ $dry_run -ne 0 ] || to_syslog_for_repo $name "successfully finished garbage collection"

  return 0
}


