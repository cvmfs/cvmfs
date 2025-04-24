#
# This file is part of the CernVM File System
# This script takes care of creating, removing, and maintaining repositories
# on a Stratum 0/1 server
#
# Implementation of the "cvmfs_server check" command

# This file depends on functions implemented in the following files:
# - cvmfs_server_util.sh
# - cvmfs_server_common.sh


__do_check() {
  local name
  local upstream
  local storage_dir
  local url

  # get repository name
  check_parameter_count_with_guessing $#
  name=$(get_or_guess_repository_name $1)

  # sanity checks
  check_repository_existence $name || die "The repository $name does not exist"

  # get repository information
  load_repo_config $name

  # more sanity checks
  is_owner_or_root $name || die "Permission denied: Repository $name is owned by $CVMFS_USER"
  health_check -r $name

  # check if repository is compatible to the installed CernVM-FS version
  check_repository_compatibility $name

  upstream=$CVMFS_UPSTREAM_STORAGE
  if is_stratum1 $name; then
    url=$CVMFS_STRATUM1
  else
    url=$CVMFS_STRATUM0
  fi

  if [ "x$tag" = "x" ] && is_garbage_collectable $name; then
    # acquire gc lock
    # waits for gc on the same repository to finish
    # and prevents a gc from starting
    acquire_gc_lock $name check || die "Failed to acquire gc lock for $name"
    trap "release_gc_lock $name" EXIT HUP INT TERM
  fi

  # do it!
  if [ $check_integrity -ne 0 ]; then
    if ! is_local_upstream $upstream; then
      echo "Storage integrity check only works locally. skipping."
    else
      echo
      echo "Checking storage integrity of $name ... (may take a while)"
      storage_dir=$(get_upstream_config $upstream)
      __swissknife scrub -r ${storage_dir}/data || die "FAIL!"
    fi
  fi

  local log_level_param=""
  local check_chunks_param=""
  [ "x$CVMFS_LOG_LEVEL" != x ] && log_level_param="-l $CVMFS_LOG_LEVEL"
  [ $check_chunks -ne 0 ]      && check_chunks_param="-c"

  local subtree_msg=""
  local subtree_param=""
  if [ "x$subtree_path" != "x" ]; then
    subtree_param="-s '$subtree_path'"
    subtree_msg=" (starting at nested catalog '$subtree_path')"
  fi

  echo "Verifying integrity of ${name}${subtree_msg}..."
  if [ $repair_reflog -eq 1 ]; then
    __check_repair_reflog $name
  fi
  local with_reflog=
  has_reflog_checksum $name && with_reflog="-R $(get_reflog_checksum $name)"

  local user_shell="$(get_user_shell $name)"
  local check_cmd
  check_cmd="$(__swissknife_cmd dbg) check $tag        \
                     $check_chunks_param               \
                     $log_level_param                  \
                     $subtree_param                    \
                     -r $url                           \
                     -t ${CVMFS_SPOOL_DIR}/tmp         \
                     -k ${CVMFS_PUBLIC_KEY}            \
                     -N ${CVMFS_REPOSITORY_NAME}       \
                     $(get_swissknife_proxy)           \
                     $(get_follow_http_redirects_flag) \
                     $with_reflog"
  $user_shell "$check_cmd"
}

# Checks for mismatch between the reflog and the checksum and tries to fix them,
# either by adjusting the checksum or by removing it.
__check_repair_reflog() {
  local name="$1"
  load_repo_config $name
  local user_shell="$(get_user_shell $name)"

  local stored_checksum=
  has_reflog_checksum $name && stored_checksum="$(cat $(get_reflog_checksum $name))"

  local repository_url=
  if is_stratum0 $name; then
    repository_url="$CVMFS_STRATUM0"
  else
    repository_url="$CVMFS_STRATUM1"
  fi

  local has_reflog=0
  local computed_checksum=
  if $user_shell "$(__swissknife_cmd) peek -d .cvmfsreflog -r $CVMFS_UPSTREAM_STORAGE" >/dev/null; then
    has_reflog=1
    local url="$repository_url/.cvmfsreflog"
    local rehash_cmd="curl -sS --fail --connect-timeout 10 --max-time 300 $(get_curl_proxy) $url \
      | cvmfs_publish hash -a ${CVMFS_HASH_ALGORITHM:-sha1}"
    computed_checksum="$($user_shell "$rehash_cmd")"
    echo "Info: found $url with content hash $computed_checksum"
  fi

  if has_reflog_checksum $name; then
    if [ $has_reflog -eq 0 ]; then
      $user_shell "rm -f $(get_reflog_checksum $name)"
      echo "Warning: removed dangling reflog checksum $(get_reflog_checksum $name)"
    else
      if [ "x$stored_checksum" != "x$computed_checksum" ]; then
        $user_shell "echo $computed_checksum > $(get_reflog_checksum $name)"
        echo "Warning: restored reflog checksum as $computed_checksum (was: $stored_checksum)"
      fi
    fi
  else
    # No checksum
    if [ $has_reflog -eq 1 ]; then
      $user_shell "echo $computed_checksum > $(get_reflog_checksum $name)"
      echo "Warning: re-created missing reflog checksum as $computed_checksum"
    fi
  fi

  # At this point we either have no .cvmfsreflog and no local reflog.chksum or
  # we have both files properly in sync.

  # Remaining case: a reflog is registered in the manifest but the
  # .cvmfsreflog file is missing.  In this case, we recreate the reflog.

  if [ $has_reflog -eq 0 ] && get_repo_info -R | grep -q ^Y; then
    echo "Warning: a reflog hash is registered in the manifest, re-creating missing reflog"
    to_syslog_for_repo $name "reference log reconstruction started"
    local repository_url

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
}

# This is a separate function because dash segfaults if it is inline :-(
__get_checks_repo_times() {
  set -- '*'
  check_parameter_count_for_multiple_repositories $#
  names=$(get_or_guess_multiple_repository_names "$@")
  check_multiple_repository_existence "$names"

  for name in $names; do
    # note that is_inactive_replica also does load_repo_config
    if is_inactive_replica $name; then
      continue
    fi

    local upstream=$CVMFS_UPSTREAM_STORAGE
    if [ x$(get_upstream_type $upstream_storage) = "xgw" ]; then
      continue
    fi

    local check_status="$(read_repo_item $name .cvmfs_status.json)"
    local last_check="$(get_json_field "$check_status" last_check)"
    local check_time=0
    if [ -n "$last_check" ]; then
      check_time="$(date --date "$last_check" +%s)"
      local min_secs num_secs
      min_secs="$((${CVMFS_CHECK_ALL_MIN_DAYS:-30}*60*60*24))"
      num_secs="$(($(date +%s)-$check_time))"
      if [ "$num_secs" -lt "$min_secs" ]; then
        # less than $CVMFS_CHECK_ALL_MIN_DAYS has elapsed since last check
        continue
      fi
    fi

    echo "${check_time}:${name}"
  done
}

__do_all_checks() {
  local log
  local repo
  local repos

  if [ ! -d /var/log/cvmfs ]; then
    if ! mkdir /var/log/cvmfs 2>/dev/null; then
      die "/var/log/cvmfs does not exist and could not create it"
    fi
  fi
  [ -w /var/log/cvmfs ] || die "cannot write to /var/log/cvmfs"

  local check_lock=/var/spool/cvmfs/is_checking_all
  if ! acquire_lock $check_lock; then
    to_syslog "skipping start of cvmfs_server check because $check_lock held by active process"
    return 1
  fi

  log=/var/log/cvmfs/checks.log

  # Sort the active repositories on local storage by last check time
  repos="$(__get_checks_repo_times|sort -n|cut -d: -f2)"

  for repo in $repos; do
    (
    to_syslog_for_repo $repo "started check"
    echo
    echo "Starting $repo at `date`"
    # Work around the errexit (that is, set -e) misfeature of being
    #  disabled whenever the exit code is to be checked.
    # See https://lists.gnu.org/archive/html/bug-bash/2012-12/msg00093.html
    set +e
    (set -e
    __do_check $repo
    )
    local ret=$?
    update_repo_status $repo last_check "`date --utc`"
    local check_status
    if [ $ret != 0 ]; then
      check_status=failed
      to_syslog_for_repo $repo "check failed"
      echo "ERROR from cvmfs_server check!" >&2
    else
      check_status=succeeded
      to_syslog_for_repo $repo "successfully completed check"
    fi
    update_repo_status $repo check_status $check_status
    echo "Finished $repo at `date`"
    ) >> $log 2>&1

  done

  release_lock $check_lock
}

cvmfs_server_check() {
  local retcode=0
  local do_all=0
  local check_chunks=1
  local check_integrity=0
  local subtree_path=""
  local tag=
  local repair_reflog=0

  # optional parameter handling
  OPTIND=1
  while getopts "acit:s:r" option
  do
    case $option in
      a)
        do_all=1
      ;;
      c)
        check_chunks=0
      ;;
      i)
        check_integrity=1
      ;;
      t)
        tag="-n $OPTARG"
      ;;
      s)
        subtree_path="$OPTARG"
      ;;
      r)
        repair_reflog=1
      ;;
      ?)
        shift $(($OPTIND-2))
        usage "Command check: Unrecognized option: $1"
      ;;
    esac
  done
  shift $(($OPTIND-1))

  if [ $do_all -eq 1 ]; then
    [ $# -eq 0 ] || die "no non-option parameters expected with -a"

    __do_all_checks

    # Always return success because this is used from cron and we
    #  don't want cron sending an email every time something fails.
    # Errors will be in the log.

  else
    if [ x"$CVMFS_LOG_LEVEL" = x ]; then
      # increase log from default "Warning" to "Info" level
      CVMFS_LOG_LEVEL=2 __do_check "$@"
    else
      __do_check "$@"
    fi
    retcode=$?
    if [ $retcode = 0 ]; then
      # Intentionally do not store the status for a failure when a check
      # is individually run, but do store the status for a success if the
      # status was previously saved.
      local name=$(get_or_guess_repository_name $1)
      local check_status="$(read_repo_item "$name" .cvmfs_status.json)"
      local last_check="$(get_json_field "$check_status" last_check)"
      if [ -n "$last_check" ]; then
        update_repo_status $name last_check "`date --utc`"
        update_repo_status $name check_status succeeded
      fi
    fi
  fi

  return $retcode
}
