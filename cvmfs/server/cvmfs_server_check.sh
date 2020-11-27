#
# This file is part of the CernVM File System
# This script takes care of creating, removing, and maintaining repositories
# on a Stratum 0/1 server
#
# Implementation of the "cvmfs_server check" command

# This file depends on fuctions implemented in the following files:
# - cvmfs_server_util.sh
# - cvmfs_server_common.sh


cvmfs_server_check() {
  local name
  local upstream
  local storage_dir
  local url
  local check_chunks=1
  local check_integrity=0
  local subtree_path=""
  local tag=
  local repair_reflog=0

  # optional parameter handling
  OPTIND=1
  while getopts "cit:s:r" option
  do
    case $option in
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

  # get repository name
  shift $(($OPTIND-1))
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

  if is_garbage_collectable $name; then
    if [ "x$tag" = "x" ]; then
      echo "Warning: if garbage collection runs in parallel, "
      echo "         missing data chunks can be falsely reported"
    fi
  fi

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
                     $(get_follow_http_redirects_flag) \
                     $with_reflog                      \
                     -z /etc/cvmfs/repositories.d/${name}/trusted_certs"
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
    local rehash_cmd="curl -sS --fail --connect-timeout 10 --max-time 300 $url \
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

  if get_repo_info -R | grep -q ^Y; then
    echo "Warning: a reflog hash is registered in the manifest, re-creating missing reflog"
    to_syslog_for_repo $name "reference log reconstruction started"
    local repository_url

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
}

