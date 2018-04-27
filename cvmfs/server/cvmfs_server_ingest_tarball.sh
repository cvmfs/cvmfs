#
# This file is part of the CernVM File System
# This script takes care of creating, removing, and maintaining repositories
# on a Stratum 0/1 server
#
# Implementation of the "cvmfs_server ingest-tarball" command

# This file depends on fuctions implemented in the following files:
# - cvmfs_server_util.sh
# - cvmfs_server_common.sh


# TODO Most of this code is replicated and shared between different scrips,
# it would be a good idea to refactor common patterns into coherent functions.

cvmfs_server_ingest_tarball() {
  local base_dir="" # where to extract the tar file
  local tar_file=""
  local to_delete="" # directories or file to delete before the extraction
  local name="" #repository name

  local force_native=0
  local force_external=0


  while [ "$2" != "" ]; do
    case $1 in
      -b | --base_dir )
        base_dir=$2
        ;;
      -t | --tar_file )
        tar_file=$2
        ;;
      -d | --delete )
        if [ "$to_delete" = "" ]
        then
          to_delete=$2
        else
          to_delete=$to_delete:$2
        fi
        ;;
    esac
    shift
  done

  name=$1
  name=$(echo $name | cut -d'/' -f1)

  if [ x"$base_dir" = "x" ]; then
    die "Please set the base directory where to extract the tarball, use -b \$BASE_DIR or --base_dir \$BASE_DIR"
  fi
  if [ x"$tar_file" = "x" ]; then
    die "Please provide the tarball to extract, use -t \$TARBALL_PATH or --tar_file \$TARBALL_PATH"
  fi

  load_repo_config $name

  upstream=$CVMFS_UPSTREAM_STORAGE
  upstream_type=$(get_upstream_type $upstream)


  is_stratum0 $name   || die "This is not a stratum 0 repository"
  is_publishing $name && die "Another publish process is active for $name"
  if [ x"$upstream_type" = xgw ]; then
      health_check -g -r $name
  else
      health_check -r $name
  fi

  user=$CVMFS_USER
  gw_key_file=/etc/cvmfs/keys/${name}.gw
  spool_dir=$CVMFS_SPOOL_DIR
  scratch_dir="${spool_dir}/scratch/current"
  stratum0=$CVMFS_STRATUM0
  hash_algorithm="${CVMFS_HASH_ALGORITHM-sha1}"
  compression_alg="${CVMFS_COMPRESSION_ALGORITHM-default}"
  if [ x"$force_compression_algorithm" != "x" ]; then
    compression_alg="$force_compression_algorithm"
  fi
  if [ x"$CVMFS_EXTERNAL_DATA" = "xtrue" -o $force_external -eq 1 ]; then
    if [ $force_native -eq 0 ]; then
      external_option="-Y"
    fi
  fi

  is_owner_or_root $name ||  die "Permission denied: Repository $name is owned by $user"
  check_repository_compatibility $name
  check_url "${CVMFS_STRATUM0}/.cvmfspublished" 20 ||  die "Repository unavailable under $CVMFS_STRATUM0"
  check_expiry $name $stratum0   || die "Repository whitelist for $name is expired!"
  is_in_transaction $name        && die "Repository $name is already in a transaction"

  [ $(count_wr_fds /cvmfs/$name) -eq 0 ] || die "Open writable file descriptors on $name"
  is_cwd_on_path "/cvmfs/$name" && die "Current working directory is in /cvmfs/$name.  Please release, e.g. by 'cd \$HOME'." || true
  gc_timespan="$(get_auto_garbage_collection_timespan $name)" || die
  if [ x"$manual_revision" != x"" ]; then
    if [ "x$(echo "$manual_revision" | tr -cd 0-9)" != "x$manual_revision" ]; then
      die "Invalid revision number: $manual_revision"
    fi
    local revision_number=$(attr -qg revision /var/spool/cvmfs/${name}/rdonly)
    if [ $manual_revision -le $revision_number ]; then
      die "Current revision '$revision_number' is ahead of manual revision number '$manual_revision'."
    fi
  fi

  if is_checked_out $name; then
    if [ x"$tag_name" = "x" ]; then
      die "Publishing a checked out revision requires a tag name"
    fi
  else
    if [ -z "$tag_name" ] && [ x"$CVMFS_AUTO_TAG" = x"true" ]; then
      local timestamp=$(date -u "+%Y-%m-%dT%H:%M:%SZ")
      tag_name="generic-$timestamp"
     local tag_name_number=1
      while check_tag_existence $name $tag_name; do
        tag_name="generic_$tag_name_number-$timestamp"
        tag_name_number=$(( $tag_name_number + 1 ))
      done
      echo "Using auto tag '$tag_name'"
    fi

    local auto_tag_cleanup_list=
    auto_tag_cleanup_list="$(filter_auto_tags $name)" || die "failed to determine outdated auto tags on $name"
  fi


  local user_shell="$(get_user_shell $name)"
  local base_hash=$(get_mounted_root_hash $name)
  local manifest="${spool_dir}/tmp/manifest"
  local dirtab_command="$(__swissknife_cmd dbg) dirtab \
    -d /cvmfs/${name}/.cvmfsdirtab                     \
    -b $base_hash                                      \
    -w $stratum0                                       \
    -t ${spool_dir}/tmp                                \
    -u /cvmfs/${name}                                  \
    -s ${scratch_dir}                                  \
    $verbosity"


  local log_level=
  [ "x$CVMFS_LOG_LEVEL" != x ] && log_level="-z $CVMFS_LOG_LEVEL"

  local trusted_certs="/etc/cvmfs/repositories.d/${name}/trusted_certs"

  local tag_command="$(__swissknife_cmd dbg) tag_edit \
    -r $upstream                                      \
    -w $stratum0                                      \
    -t ${spool_dir}/tmp                               \
    -m $manifest                                      \
    -p /etc/cvmfs/keys/${name}.pub                    \
    -f $name                                          \
    -e $hash_algorithm                                \
    $(get_follow_http_redirects_flag)"
  if ! is_checked_out $name; then
    # enables magic undo tag handling
    tag_command="$tag_command -x"
  else
    tag_command="$tag_command -B $(get_checked_out_branch $name)"
    if [ "x$(get_checked_out_previous_branch $name)" != "x" ]; then
      tag_command="$tag_command -P $(get_checked_out_previous_branch $name)"
    fi
  fi
  if [ ! -z "$tag_name" ]; then
    tag_command="$tag_command -a $tag_name"
  fi
  if [ ! -z "$tag_channel" ]; then
    tag_command="$tag_command -c $tag_channel"
  fi
  if [ ! -z "$tag_description" ]; then
    tag_command="$tag_command -D \"$tag_description\""
  fi

  local tag_command_undo_tags="$(__swissknife_cmd dbg) tag_edit \
    -r $upstream                                                \
    -w $stratum0                                                \
    -t ${spool_dir}/tmp                                         \
    -m $manifest                                                \
    -p /etc/cvmfs/keys/${name}.pub                              \
    -f $name                                                    \
    -e $hash_algorithm                                          \
    $(get_follow_http_redirects_flag)                           \
    -x"


  local ingest_tarball_command="$(__swissknife_cmd dbg) \
    ingest_tarball                                 \
    -u /cvmfs/$name                                \
    -s ${scratch_dir}                              \
    -c ${spool_dir}/rdonly                         \
    -t ${spool_dir}/tmp                            \
    -b $base_hash                                  \
    -r ${upstream}                                 \
    -w $stratum0                                   \
    -o $manifest                                   \
    -K $CVMFS_PUBLIC_KEY                           \
    -N $name                                       \
    -T $tar_file                                   \
    -B $base_dir                                   \
    "

  if [ ! -z "$to_delete" ]; then
    ingest_tarball_command="$ingest_tarball_command -D $to_delete"
  fi


  # ---> do it! (from here on we are changing things)
  publish_before_hook $name
  $user_shell "$dirtab_command" || die "Failed to apply .cvmfsdirtab"

  # check if we have open file descriptors on /cvmfs/<name>
  local use_fd_fallback=0
  handle_read_only_file_descriptors_on_mount_point $name $open_fd_dialog || use_fd_fallback=1

  publish_starting $name

  echo "Command being run"
  echo $ingest_tarball_command

  $user_shell "$ingest_tarball_command" || { publish_failed $name; die "Synchronization failed\n\nExecuted Command:\n$sync_command";   }

  cvmfs_sys_file_is_regular $manifest            || { publish_failed $name; die "Manifest creation failed\n\nExecuted Command:\n$sync_command"; }

  local branch_hash=
  local trunk_hash=$(grep "^C" $manifest | tr -d C)
  if is_checked_out $name; then
    local branch_hash=$trunk_hash
    trunk_hash=$(get_published_root_hash $name)
    tag_command="$tag_command -h $branch_hash"
    # write intermediate catalog hash to reflog
    sign_manifest $name $manifest "" true
    # Replace throw-away manifest with upstream copy
    get_raw_manifest $name > $manifest
    cvmfs_sys_file_is_empty $manifest && die "failed to reload manifest"
  fi

  if [ x"$upstream_type" = xgw ]; then
      close_transaction  $name $use_fd_fallback
      publish_after_hook $name
      publish_succeeded $name
      echo "Changes submitted to repository gateway"
      return 0
  fi

  # Remove outdated automatically created tags
  local tag_remove_cmd_file=
  if [ ! -z "$auto_tag_cleanup_list" ]; then
    local tag_list_file=$(mktemp)
    echo $auto_tag_cleanup_list | xargs -n100 echo > $tag_list_file
    tag_remove_cmd_file=$(mktemp)
    cat $tag_list_file | while read REPLY; do
      local tag_cleanup_command="$(__swissknife_cmd dbg) tag_edit \
        -r $upstream                                        \
        -w $stratum0                                        \
        -t ${spool_dir}/tmp                                 \
        -m $manifest                                        \
        -p /etc/cvmfs/keys/${name}.pub                      \
        -f $name                                            \
        -b $base_hash                                       \
        -e $hash_algorithm                                  \
        $(get_follow_http_redirects_flag)                   \
        -d \\\"$REPLY\\\""
      echo $user_shell \"${tag_cleanup_command}\" >> $tag_remove_cmd_file
    done
    rm -f $tag_list_file
  fi

  if [ ! -z "$tag_remove_cmd_file" ]; then
    echo "Removing outdated automatically generated tags for $name..."
    /bin/sh $tag_remove_cmd_file || \
      { rm -f $tag_remove_cmd_file; publish_failed $name; \
        die "Removing tags failed\n\nExecuted Command:\n/bin/sh \
        $tag_remove_cmd_file"; }
    rm -f $tag_remove_cmd_file
    # write intermediate history hash to reflog
    sign_manifest $name $manifest "" true
  fi

  # add a tag for the new revision
  echo "Tagging $name"
  $user_shell "$tag_command" || { publish_failed $name; die "Tagging failed\n\nExecuted Command:\n$tag_command";  }

  if [ "x$sync_command_virtual_dir" != "x" ]; then
    # write intermediate catalog hash and history to reflog
    sign_manifest $name $manifest "" true
    $user_shell "$sync_command_virtual_dir" || { publish_failed $name; die "Editing .cvmfs failed\n\nExecuted Command:\n$sync_command_virtual_dir";  }
    local trunk_hash=$(grep "^C" $manifest | tr -d C)
    $user_shell "$tag_command_undo_tags" || { publish_failed $name; die "Creating undo tags\n\nExecuted Command:\n$tag_command_undo_tags";  }
  fi

  # finalizing transaction
  echo "Flushing file system buffers"
  sync

  # committing newly created revision
  echo "Signing new manifest"
  sign_manifest $name $manifest      || { publish_failed $name; die "Signing failed"; }
  set_ro_root_hash $name $trunk_hash || { publish_failed $name; die "Root hash update failed"; }
  if is_checked_out $name; then
    rm -f /var/spool/cvmfs/${name}/checkout
    echo "Reset to trunk on default branch"
  fi

  # run the automatic garbage collection (if configured)
  if has_auto_garbage_collection_enabled $name; then
    echo "Running automatic garbage collection"
    local dry_run=0
    __run_gc $name       \
             $stratum0   \
             $dry_run    \
             ""          \
             "0"         \
             -z $gc_timespan      || { local err=$?; publish_failed $name; die "Garbage collection failed ($err)"; }
  fi

  # check again for open file descriptors (potential race condition)
  if has_file_descriptors_on_mount_point $name && \
     [ $use_fd_fallback -ne 1 ]; then
    file_descriptor_warning $name
    echo "Forcing remount of already committed repository revision"
    use_fd_fallback=1
  else
    echo "Remounting newly created repository revision"
  fi

  # remount the repository
  remount_repo  $name $use_fd_fallback
  publish_after_hook $name
  publish_succeeded  $name

}
