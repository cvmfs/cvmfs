#
# This file is part of the CernVM File System
# This script takes care of creating, removing, and maintaining repositories
# on a Stratum 0/1 server
#
# Implementation of the "cvmfs_server gc" command

# This file depends on fuctions implemented in the following files:
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
  local deletion_log=""
  local reconstruct_reflog="0"

  # optional parameter handling
  OPTIND=1
  while getopts "ldr:t:fL:" option
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
  check_parameter_count_for_multiple_repositories $#
  names=$(get_or_guess_multiple_repository_names "$@")
  check_multiple_repository_existence "$names"

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

  # sanity checks
  if [ $dry_run -ne 0 ] && [ $reconstruct_reflog -ne 0 ]; then
    die "Reflog reconstruction needed. Cannot do a dry-run."
  fi

  # safety user confirmation
  if [ $force -eq 0 ] && [ $dry_run -eq 0 ]; then
    echo "YOU ARE ABOUT TO DELETE DATA! Are you sure you want to do the following:"
  fi

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
    echo "Preserve Revisions newer than: $(date -d@$preserve_timestamp +'%x %X')"
  fi
  if [ $preserve_revisions -le 0 ] && [ $preserve_timestamp -le 0 ]; then
    echo "Only the latest revision will be preserved."
  fi

  if [ $force -eq 0 ]; then
    echo ""
    read -p "Please confirm this action (y/N)? " reply
    if [ "$reply" != "y" ] && [ "$reply" != "Y" ]; then
      return 1
    fi
  fi

  for name in $names; do

    load_repo_config $name

    # sanity checks
    check_repository_compatibility $name
    if is_empty_repository $name; then
      echo "Repository $name is empty, nothing to do"
      continue
    fi
    is_garbage_collectable $name || die "Garbage Collection is not enabled for $name"
    is_owner_or_root       $name || die "Permission denied: Repository $name is owned by $user"
    is_in_transaction      $name && die "Cannot run garbage collection while in a transaction"

    local head_timestamp="$(get_repo_info -t)"
    [ $head_timestamp -gt $preserve_timestamp ] || die "Latest repository revision is older than given timestamp"

    # figure out the URL of the repository
    local repository_url="$CVMFS_STRATUM0"
    if is_stratum1 $name; then
      [ ! -z $CVMFS_STRATUM1 ] || die "Missing CVMFS_STRATUM1 URL in server.conf"
      repository_url="$CVMFS_STRATUM1"
    fi

    # generate the garbage collection configuration
    local additional_switches=""
    [ $list_deleted_objects -ne 0 ] && additional_switches="$additional_switches -l"
    [ $dry_run              -ne 0 ] && additional_switches="$additional_switches -d"
    [ $preserve_revisions   -ge 0 ] && additional_switches="$additional_switches -h $preserve_revisions"
    [ $preserve_timestamp   -gt 0 ] && additional_switches="$additional_switches -z $preserve_timestamp"

    # retrieve the base hash of the repository to be editied
    local base_hash=""
    local manifest=""

    # gather extra information for a stratum0 repository and open a transaction
    if is_stratum0 $name; then
      base_hash="$(get_mounted_root_hash $name)"
      manifest="${CVMFS_SPOOL_DIR}/tmp/manifest"
    fi

    if [ $dry_run -eq 0 ]; then
      if is_stratum0 $name; then
        trap "close_transaction $name 0" EXIT HUP INT TERM
        open_transaction $name || die "Failed to open transaction for garbage collection"
      else
        acquire_update_lock $name gc || die "Failed to acquire update lock for garbage collection"
        trap "release_update_lock $name" EXIT HUP INT TERM
      fi
    fi

    to_syslog_for_repo $name "started manual garbage collection"

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
             "$manifest"                \
             "$base_hash"               \
             "$deletion_log"            \
             "$reconstruct_this_reflog" \
             $additional_switches || die "Fail ($?)!"

    if [ $dry_run -eq 0 ]; then
      # sign the result
      if is_stratum0 $name; then
        echo "Signing Repository Manifest"
        if ! sign_manifest $name $manifest; then
          to_syslog_for_repo $name "failed to sign manifest after manual garbage collection"
          die "Fail!"
        fi

        # close the transaction
        trap - EXIT HUP INT TERM
        close_transaction $name 0
      else
        # release the update lock
        trap - EXIT HUP INT TERM
        release_update_lock $name
      fi
    fi

    to_syslog_for_repo $name "successfully finished manual garbage collection"

  done
}

__run_gc() {
  local name="$1"
  local repository_url="$2"
  local dry_run="$3"
  local manifest="$4"
  local base_hash="$5"
  local deletion_log="$6"
  local reconstruct_reflog="$7"
  shift 7
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

  if is_stratum0 $name; then
    [ x"$manifest"  != x"" ] || return 4
    [ x"$base_hash" != x"" ] || return 5
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

  # do it!
  local user_shell="$(get_user_shell $name)"

  if [ $reconstruct_reflog -ne 0 ]; then
    to_syslog_for_repo $name "reference log reconstruction started"
    local reflog_reconstruct_command="$(__swissknife_cmd dbg) reconstruct_reflog \
                                                  -r $repository_url             \
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

  local hash_algorithm="${CVMFS_HASH_ALGORITHM-sha1}"
  if is_stratum0 $name && [ $dry_run -eq 0 ]; then
    tag_command="$(__swissknife_cmd dbg) tag_empty_bin \
      -r $CVMFS_UPSTREAM_STORAGE                       \
      -w $CVMFS_STRATUM0                               \
      -t ${CVMFS_SPOOL_DIR}/tmp                        \
      -m $manifest                                     \
      -p /etc/cvmfs/keys/${name}.pub                   \
      -f $name                                         \
      -b $base_hash                                    \
      -e $hash_algorithm"
    if ! $user_shell "$tag_command"; then
      to_syslog_for_repo $name "failed to update history after garbage collection"
      return 7
    fi
  fi

  [ $dry_run -ne 0 ] || update_repo_status $name last_gc "`date --utc`"
  [ $dry_run -ne 0 ] || to_syslog_for_repo $name "successfully finished garbage collection"

  return 0
}


