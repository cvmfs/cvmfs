#
# This file is part of the CernVM File System
# This script takes care of creating, removing, and maintaining repositories
# on a Stratum 0/1 server
#
# Implementation of the "cvmfs_server overlay" command

# This file depends on functions implemented in the following files:
# - cvmfs_server_util.sh
# - cvmfs_server_common.sh


# Mountless gateway overlay helpers.  These mirror the equivalents in
# cvmfs_server_ingest.sh: a mountless publisher has no FUSE overlay to
# open/close, so the transaction is just a gateway lease that is acquired up
# front and dropped (or committed by the merge) at the end.
# TODO: these are duplicated from cvmfs_server_ingest.sh; both should move to
# cvmfs_server_common.sh (see the refactor TODO at the top of that script).
cvmfs_server_overlay_release_gateway_lease() {
  local name="$1"
  local gateway_api_url="$2"
  local gw_key_file="$3"
  local lease_path="$4"
  local session_token_file="/var/spool/cvmfs/${name}/session_token"

  cvmfs_sys_file_is_regular "$session_token_file" || return 0
  cvmfs_swissknife lease -u "$gateway_api_url" -a drop \
    -k "$gw_key_file" -p "$lease_path"
}


cvmfs_server_overlay_abort_transaction() {
  local name="$1"
  local mountless_gateway_overlay="$2"
  local gateway_api_url="$3"
  local gw_key_file="$4"
  local lease_path="$5"

  if [ $mountless_gateway_overlay -eq 1 ]; then
    cvmfs_server_overlay_release_gateway_lease \
      "$name" "$gateway_api_url" "$gw_key_file" "$lease_path" \
      >/dev/null 2>&1 || true
  else
    cvmfs_server_abort -f "$name"
  fi
}


# Failure handler for the mountless gateway overlay path.  Mirrors
# publish_failed but skips the run_suid_helper open/close calls that require a
# FUSE overlay.
cvmfs_server_overlay_publish_failed_mountless() {
  local name="$1"
  local gateway_api_url="$2"
  local gw_key_file="$3"
  local lease_path="$4"
  load_repo_config $name
  local pub_lock="${CVMFS_SPOOL_DIR}/is_publishing"
  trap - EXIT HUP INT TERM
  cvmfs_server_overlay_release_gateway_lease \
    "$name" "$gateway_api_url" "$gw_key_file" "$lease_path" \
    >/dev/null 2>&1 || true
  release_lock $pub_lock
  to_syslog_for_repo $name "failed to publish"
}


# Like publish_starting but for the mountless gateway overlay path: acquire the
# publishing lock and set an EXIT trap that includes gateway lease cleanup,
# omitting the run_suid_helper lock call that needs a FUSE overlay.
cvmfs_server_overlay_publish_starting_mountless() {
  local name="$1"
  local gateway_api_url="$2"
  local gw_key_file="$3"
  local lease_path="$4"
  load_repo_config $name
  local pub_lock="${CVMFS_SPOOL_DIR}/is_publishing"
  acquire_lock "$pub_lock" || {
    cvmfs_server_overlay_release_gateway_lease \
      "$name" "$gateway_api_url" "$gw_key_file" "$lease_path" \
      >/dev/null 2>&1 || true
    die "Failed to acquire publishing lock"
  }
  trap "cvmfs_server_overlay_publish_failed_mountless '$name' '$gateway_api_url' '$gw_key_file' '$lease_path'" EXIT HUP INT TERM
  to_syslog_for_repo $name "started publishing"
}


# Combined failure handler called from error blocks AFTER publish_starting (or
# its mountless variant).  Drops the gateway lease for mountless, aborts the
# FUSE-backed transaction otherwise.
cvmfs_server_overlay_fail() {
  local name="$1"
  local mountless_gateway_overlay="$2"
  local gateway_api_url="$3"
  local gw_key_file="$4"
  local lease_path="$5"
  if [ $mountless_gateway_overlay -eq 1 ]; then
    cvmfs_server_overlay_publish_failed_mountless \
      "$name" "$gateway_api_url" "$gw_key_file" "$lease_path"
  else
    publish_failed "$name"
    cvmfs_server_abort -f "$name"
  fi
}


cvmfs_server_overlay() {
  local layers=""
  local dest_path=""
  local name=""
  local oci_config=""
  local skip_singularity=0

  while [ "$2" != "" ]; do
    case $1 in
      -l | --layers )
        layers=$2
        ;;
      -d | --dest )
        dest_path=$2
        # remove any duplicated slashes in pathname
        dest_path=$(echo $dest_path | tr -s / )
        ;;
      -c | --config )
        oci_config=$2
        ;;
      -S | --skip-singularity )
        skip_singularity=1
        ;;
    esac
    shift
  done

  name=$1

  if [ x"$name" = "x" ]; then
    die "Please provide a repository name"
  fi

  if [ x"$layers" = "x" ]; then
    die "Please provide layer paths with -l (comma-separated, bottom-to-top order)"
  fi

  if [ x"$dest_path" = "x" ]; then
    die "Please provide a destination path with -d"
  fi

  load_repo_config $name

  # sanity checks
  is_stratum0 $name   || die "This is not a stratum 0 repository"
  is_publishing $name && die "Another publish process is active for $name"
  health_check -g -r $name
  is_owner_or_root $name || die "Permission denied: Repository $name is owned by $CVMFS_USER"
  check_repository_compatibility $name

  local upstream=$CVMFS_UPSTREAM_STORAGE
  local upstream_type=$(get_upstream_type $upstream)

  local spool_dir=$CVMFS_SPOOL_DIR
  local gw_key_file=/etc/cvmfs/keys/${name}.gw
  local mountless_gateway_overlay=0
  local gateway_api_url=""
  local gateway_lease_path=""

  # Open a transaction for the overlay operation.  When the upstream is a
  # gateway and there is no FUSE mount (mountless publisher), acquire the
  # gateway lease directly instead of going through cvmfs_server_transaction,
  # which would try to (re)mount the publisher overlay.
  if [ x"$upstream_type" = xgw ]; then
    gateway_lease_path="$name/$dest_path"
    if ! is_mounted "/cvmfs/$name" && ! is_mounted "${CVMFS_SPOOL_DIR}/rdonly"; then
      mountless_gateway_overlay=1
      if is_checked_out $name; then
        die "Mountless gateway overlay requires a mounted publisher for checked-out repositories."
      fi
      gateway_api_url=$(get_upstream_config "$upstream")
      cvmfs_swissknife lease -u "$gateway_api_url" -a acquire \
        -k "$gw_key_file" -p "$gateway_lease_path" || \
        die "Impossible to start a transaction"
    else
      cvmfs_server_transaction "$gateway_lease_path" || die "Impossible to start a transaction"
    fi
  else
    cvmfs_server_transaction $name || die "Impossible to start a transaction"
  fi

  # Abort/fail helpers, defined once the gateway variables are known.
  _abort() {
    cvmfs_server_overlay_abort_transaction \
      "$name" "$mountless_gateway_overlay" "$gateway_api_url" \
      "$gw_key_file" "$gateway_lease_path"
  }
  _fail() {
    cvmfs_server_overlay_fail \
      "$name" "$mountless_gateway_overlay" "$gateway_api_url" \
      "$gw_key_file" "$gateway_lease_path"
  }

  local stratum0=$CVMFS_STRATUM0
  local hash_algorithm="${CVMFS_HASH_ALGORITHM-sha1}"
  local compression_alg="${CVMFS_COMPRESSION_ALGORITHM:-default}"

  local user_shell="$(get_user_shell $name)"
  local base_hash=
  if [ $mountless_gateway_overlay -eq 1 ]; then
    base_hash=$(get_published_root_hash $name) || { _abort; die "Failed to get published root hash for $name"; }
  else
    base_hash=$(get_mounted_root_hash $name)
  fi
  local manifest="${spool_dir}/tmp/manifest"

  # Build the swissknife overlay command
  local overlay_command="$(__swissknife_cmd dbg) \
    overlay                                      \
    -r ${upstream}                               \
    -w $stratum0                                 \
    -t ${spool_dir}/tmp                          \
    -o $manifest                                 \
    -b $base_hash                                \
    -K $CVMFS_PUBLIC_KEY                         \
    -N $name                                     \
    -l $layers                                   \
    -d $dest_path                                \
    -e $hash_algorithm                           \
    -Z $compression_alg                          \
    $(get_swissknife_proxy)                      \
    $(get_follow_http_redirects_flag)             \
    $([ -n "$oci_config" ] && echo "-c $oci_config") \
    $([ $skip_singularity -eq 1 ] && echo "-S")"

  # For a gateway upstream the merge is committed through the lease, so pass
  # the gateway key and session token (mirrors cvmfs_server ingest).  The
  # session token file is created by the lease acquire above (mountless) or by
  # cvmfs_server_transaction (mounted gateway).
  if [ x"$upstream_type" = xgw ]; then
    overlay_command="$overlay_command -H $gw_key_file -P ${spool_dir}/session_token"
  fi

  # ---> do it!
  publish_before_hook $name

  # check if we have open file descriptors on /cvmfs/<name> (only meaningful
  # when there is a FUSE mount, i.e. not in the mountless gateway path)
  local use_fd_fallback=0
  if [ $mountless_gateway_overlay -ne 1 ]; then
    [ $(count_wr_fds /cvmfs/$name) -eq 0 ] || { _abort; die "Open writable file descriptors on $name"; }
    handle_read_only_file_descriptors_on_mount_point $name $open_fd_dialog || use_fd_fallback=1
  fi

  if [ $mountless_gateway_overlay -eq 1 ]; then
    cvmfs_server_overlay_publish_starting_mountless \
      "$name" "$gateway_api_url" "$gw_key_file" "$gateway_lease_path"
  else
    publish_starting $name
  fi

  $user_shell "$overlay_command" || { _fail; die "Overlay merge failed\n\nExecuted Command:\n$overlay_command"; }
  cvmfs_sys_file_is_regular $manifest || { _fail; die "Manifest creation failed"; }

  local trunk_hash=$(grep "^C" $manifest | tr -d C)

  if [ x"$upstream_type" = xgw ]; then
    if [ $mountless_gateway_overlay -ne 1 ]; then
      close_transaction $name $use_fd_fallback
    else
      # No FUSE overlay to close; the merge already committed the gateway lease
      # (FinalizeSession(true)).  Only the local session_token file remains.
      rm -f "${spool_dir}/session_token"
    fi
    publish_after_hook $name
    publish_succeeded $name
    echo "Changes submitted to repository gateway"
    return 0
  fi

  # Tag the new revision
  local tag_command="$(__swissknife_cmd dbg) tag_edit \
    -r $upstream                                      \
    -w $stratum0                                      \
    -t ${spool_dir}/tmp                               \
    -m $manifest                                      \
    -p /etc/cvmfs/keys/${name}.pub                    \
    -f $name                                          \
    -e $hash_algorithm                                \
    $(get_swissknife_proxy)                           \
    $(get_follow_http_redirects_flag)                 \
    -x"

  echo "Tagging $name"
  $user_shell "$tag_command" || { publish_failed $name; cvmfs_server_abort -f $name; die "Tagging failed\n\nExecuted Command:\n$tag_command"; }


  echo "Flushing file system buffers"
  sync
  # Finalize
  echo "Signing new manifest"
  sign_manifest $name $manifest      || { publish_failed $name; cvmfs_server_abort -f $name; die "Signing failed"; }
  set_ro_root_hash $name $trunk_hash || { publish_failed $name; cvmfs_server_abort -f $name; die "Root hash update failed"; }

  # check again for open file descriptors (potential race condition)
  if has_file_descriptors_on_mount_point $name && \
     [ $use_fd_fallback -ne 1 ]; then
    file_descriptor_warning $name
    echo "Forcing remount of already committed repository revision"
    use_fd_fallback=1
  else
    echo "Remounting newly created repository revision"
  fi

  close_transaction $name $use_fd_fallback
  publish_after_hook $name
  publish_succeeded  $name
}

