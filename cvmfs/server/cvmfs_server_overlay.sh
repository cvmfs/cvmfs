#
# This file is part of the CernVM File System
# This script takes care of creating, removing, and maintaining repositories
# on a Stratum 0/1 server
#
# Implementation of the "cvmfs_server overlay" command

# This file depends on functions implemented in the following files:
# - cvmfs_server_util.sh
# - cvmfs_server_common.sh


cvmfs_server_overlay() {
  local layers=""
  local dest_path=""
  local name=""

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

  # Open a transaction for the overlay operation
  if [ x"$upstream_type" = xgw ]; then
    cvmfs_server_transaction "$name/$dest_path" || die "Impossible to start a transaction"
  else
    cvmfs_server_transaction $name || die "Impossible to start a transaction"
  fi

  local spool_dir=$CVMFS_SPOOL_DIR
  local stratum0=$CVMFS_STRATUM0
  local hash_algorithm="${CVMFS_HASH_ALGORITHM-sha1}"
  local compression_alg="${CVMFS_COMPRESSION_ALGORITHM-default}"

  local user_shell="$(get_user_shell $name)"
  local base_hash=$(get_mounted_root_hash $name)
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

  # ---> do it!
  publish_before_hook $name

  # check if we have open file descriptors on /cvmfs/<name>
  [ $(count_wr_fds /cvmfs/$name) -eq 0 ] || { cvmfs_server_abort -f $name; die "Open writable file descriptors on $name"; }
  local use_fd_fallback=0
  handle_read_only_file_descriptors_on_mount_point $name $open_fd_dialog || use_fd_fallback=1

  publish_starting $name

  $user_shell "$overlay_command" || { publish_failed $name; cvmfs_server_abort -f $name; die "Overlay merge failed\n\nExecuted Command:\n$overlay_command"; }
  cvmfs_sys_file_is_regular $manifest || { publish_failed $name; cvmfs_server_abort -f $name; die "Manifest creation failed"; }

  local trunk_hash=$(grep "^C" $manifest | tr -d C)

  if [ x"$upstream_type" = xgw ]; then
    close_transaction $name $use_fd_fallback
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

