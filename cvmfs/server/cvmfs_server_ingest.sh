#
# This file is part of the CernVM File System
# This script takes care of creating, removing, and maintaining repositories
# on a Stratum 0/1 server
#
# Implementation of the "cvmfs_server ingest-tarball" command

# This file depends on functions implemented in the following files:
# - cvmfs_server_util.sh
# - cvmfs_server_common.sh


# TODO Most of this code is replicated and shared between different scripts,
# it would be a good idea to refactor common patterns into coherent functions.

cvmfs_server_ingest() {
  local base_dir="" # where to extract the tar file
  local tar_file=""
  local to_delete="" # directories or file to delete before the extraction
  local name="" #repository name
  local user=""
  local group=""
  local uid=""
  local gid=""
  local keep_ownership=false
  local create_catalog=false

  local force_native=0
  local force_external=0

  # if we use the gateway we cannot easily accept multiple deletion
  local multiple_delete=0

  local name_from_absolute_arg=""
  local name_from_absolute_arg2=""

  while [ "$2" != "" ]; do
    case $1 in
      -b | --base_dir )
        base_dir=$2
        # remove any duplicated slashes in pathname
        # swissknife cannot handle it at the moment
        base_dir=$(echo $base_dir | tr -s / )
        ;;
      -t | --tar_file )
        tar_file=$2
        ;;
      -d | --delete )
        if [ "x$to_delete" = "x" ]
        then
          to_delete=$2
        else
          to_delete=$to_delete:$2
          multiple_delete=1
        fi
        ;;
      -c | --catalog )
        create_catalog=true
        ;;
      -u | --user )
        user=$2
      ;;
      -g | --group )
        group=$2
      ;;
      -k | --keep-ownership )
        keep_ownership=true
      ;;
    esac
    shift
  done

  # deal with absolute/relative paths
  case x"$base_dir" in
      x/cvmfs/*) 
        echo "Warning: interpreting the base_dir as absolute path. Remove leading slash to get a relative path to the mountpoint"
        name_from_absolute_arg=$(echo $base_dir | cut -d'/' -f3)
        base_dir=$(echo $base_dir | cut -d'/' -f 4-)
  esac
  for to_delete_path in $(echo $to_delete | sed "s/:/ /g"); do
    case x"$to_delete_path" in
        x/cvmfs/*) 
          echo "Warning: interpreting the base_dir as absolute path. Remove leading slash to get a relative path to the mountpoint"
          name_from_absolute_arg2=$(echo $to_delete_path | cut -d'/' -f3)
          to_delete=$(echo $to_delete_path | cut -d'/' -f 4-)
          if [ ! x$name_from_absolute_arg = "x" ] ; then
            if [ ! x$name_from_absolute_arg = x$name_from_absolute_arg2 ] ; then
              die "Cannot use different repositories in same transaction: $name_from_absolute_arg2, $name_from_absolute_arg"
            fi
          fi
          name_from_absolute_arg=$name_from_absolute_arg2
    esac
  done

  name=$1
  name=$(echo $name | cut -d'/' -f1)

  if [ x"$name" = "x" ] ; then
    name=$name_from_absolute_arg
  fi
  echo "Info: transaction on repository $name"

  if [ x"$name" = "x" ] ; then

    die "Please provide a repository name, as positional argument or via an absolute path on /cvmfs given to -b"
  fi

  if [ x"$tar_file" = "x" ] && [ x"$base_dir" = "x" ] && [ x"$to_delete" = "x" ] ; then
    die "Please provide some parameters, use -t \$TAR_FILE to provide the tar to extract -b \$BASE_DIR to provide where to extract the tar and -d \$TO_DELETE to provide what to delete from the repository"
  fi

  if [ x"$tar_file" = "x" ] && [ ! x"$base_dir" = "x" ]; then
    die "Please provide the tarball to extract, use -t \$TARBALL_PATH or --tar_file \$TARBALL_PATH or don't provide the base directory to simply delete entities from the repository"
  fi

  if [ ! x"$tar_file" = "x" ] && [ x"$base_dir" = "x" ]; then
    die "Please set the base directory where to extract the tarball, use -b \$BASE_DIR or --base_dir \$BASE_DIR or don't provide the base directory to simply delete entities from the repository"
  fi

  load_repo_config $name

  #### check and set uid/gid
  # error: cannot keep ownership while also requesting other user/group
  if { [ x"$user" != "x" ] || [ x"$group" != "x" ]; } && [ $keep_ownership = true ]; then
    die "You cannot provide both: either provide user (-u)/group (-g) or keep the ownership (-k) of the tarball"
  fi


  # error: group also needs user
  if [ x"$user" = "x" ] && [ x"$group" != "x" ]; then
    die "If providing a group name, you also must provide a user (use -u) to set new owner of the ingest tarball"
  fi

  # both set
  if [ x"$user" != "x" ]; then
    uid=$(id -u "$user")

    if [ x"$uid" = xi* ]; then
      die "User set but no valid user name given"
    fi
  fi

  if [ x"$group" != "x" ]; then
    gid=$(getent group "$group" | awk -F':' '{print $3;}')

    if [ x"$gid" = "x" ]; then
      die "Group set but no valid group name given"
    fi
  fi
  # only user set: get gid from user
  if [ x"$group" != "x" ]; then
    gid=$(id -g "$user")
  fi
  # use default cvmfs repo owner
  if [ x"$user" = "x" ] && [ x"$group" = "x" ] && [ $keep_ownership = false ]; then
    uid=$(id -u "$CVMFS_USER")
    gid=$(id -g "$CVMFS_USER")

    if [ x"$uid" = xi* ]; then
      die "Default CVMFS_USER $CVMFS_USER for the repo does not exist"
    fi
  fi
  # keep tar ball ownership
  if [ $keep_ownership = true ]; then
    uid="-1"
    gid="-1"
  fi

  upstream=$CVMFS_UPSTREAM_STORAGE
  upstream_type=$(get_upstream_type $upstream)

  if [ x"$upstream_type" = xgw ]; then

    if [ $multiple_delete -eq 1 ]; then
      die "Could not delete multiple paths using a gateway in a single transaction."
    fi

    if [ ! x"$tar_file" = "x" ] && [ ! x"$to_delete" = "x" ]; then
      die "Could not delete and add a file in the same transaction while using gateway."
    fi
    # by the check above we are sure that there is only a tar_file to ingest or a directory to_delete
    # hence we just concatenate them with the name for the transaction
    cvmfs_server_transaction "$name/$base_dir$to_delete" || die "Impossible to start a transaction"
  else
    cvmfs_server_transaction $name || die "Impossible to start a transaction"
  fi

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


  [ $(count_wr_fds /cvmfs/$name) -eq 0 ] || { cvmfs_server_abort -f $name; die "Open writable file descriptors on $name"; }
  is_cwd_on_path "/cvmfs/$name" && { cvmfs_server_abort -f $name; die "Current working directory is in /cvmfs/$name.  Please release, e.g. by 'cd \$HOME'."; } || true
  gc_timespan="$(get_auto_garbage_collection_timespan $name)" || { cvmfs_server_abort -f $name; die; }
  if [ x"$manual_revision" != x"" ]; then
    if [ "x$(echo "$manual_revision" | tr -cd 0-9)" != "x$manual_revision" ]; then
      cvmfs_server_abort -f $name
      die "Invalid revision number: $manual_revision"
    fi
    local revision_number=$(attr -qg revision /var/spool/cvmfs/${name}/rdonly)
    if [ $manual_revision -le $revision_number ]; then
      cvmfs_server_abort -f $name
      die "Current revision '$revision_number' is ahead of manual revision number '$manual_revision'."
    fi
  fi

  if is_checked_out $name; then
    if [ x"$tag_name" = "x" ]; then
      cvmfs_server_abort -f $name
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
    auto_tag_cleanup_list="$(filter_auto_tags $name)" || { cvmfs_server_abort -f $name; die "failed to determine outdated auto tags on $name"; }
  fi


  local user_shell="$(get_user_shell $name)"
  local base_hash=$(get_mounted_root_hash $name)
  local manifest="${spool_dir}/tmp/manifest"
  local dirtab_command="$(__swissknife_cmd dbg) dirtab \
    -d /cvmfs/${name}/.cvmfsdirtab                     \
    -b $base_hash                                      \
    -w $stratum0                                       \
    $(get_swissknife_proxy)                            \
    -t ${spool_dir}/tmp                                \
    -u /cvmfs/${name}                                  \
    -s ${scratch_dir}                                  \
    $verbosity"


  local log_level=
  [ "x$CVMFS_LOG_LEVEL" != x ] && log_level="-z $CVMFS_LOG_LEVEL"

  local tag_command="$(__swissknife_cmd dbg) tag_edit \
    -r $upstream                                      \
    -w $stratum0                                      \
    -t ${spool_dir}/tmp                               \
    -m $manifest                                      \
    -p /etc/cvmfs/keys/${name}.pub                    \
    -f $name                                          \
    -e $hash_algorithm                                \
    $(get_swissknife_proxy)                           \
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
    $(get_swissknife_proxy)                                     \
    $(get_follow_http_redirects_flag)                           \
    -x"


  local ingest_command="$(__swissknife_cmd dbg) \
    ingest                                      \
    -u /cvmfs/$name                             \
    -c ${spool_dir}/rdonly                      \
    -t ${spool_dir}/tmp                         \
    -b $base_hash                               \
    -r ${upstream}                              \
    -w $stratum0                                \
    $(get_swissknife_proxy)                     \
    -o $manifest                                \
    -K $CVMFS_PUBLIC_KEY                        \
    -N $name                                    \
    -U $uid                                     \
    -G $gid                                     \
    "

  if [ ! x"$tar_file" = "x" ]; then
    ingest_command="$ingest_command -T $tar_file"
  fi

  if [ ! x"$base_dir" = "x" ]; then
    ingest_command="$ingest_command -B $base_dir"
  fi

  if [ ! x"$to_delete" = "x" ]; then
    ingest_command="$ingest_command -D $to_delete"
  fi

  if [ "$create_catalog" = true ]; then
    ingest_command="$ingest_command -C true"
  fi

  if [ "x$CVMFS_ENABLE_MTIME_NS" = "xtrue" ]; then
    ingest_command="$ingest_command -j"
  fi

  if [ "x$CVMFS_PRINT_STATISTICS" = "xtrue" ]; then
    ingest_command="$ingest_command -+stats"
  fi

  if [ "x$CVMFS_UPLOAD_STATS_DB" = "xtrue" ]; then
    ingest_command="$ingest_command -I"
  fi

  local upstream_storage=$CVMFS_UPSTREAM_STORAGE
  local upstream_type=$(get_upstream_type $upstream_storage)
  gw_key_file=/etc/cvmfs/keys/${name}.gw

  if [ x"$upstream_type" = xgw ]; then
    ingest_command="$ingest_command -H $gw_key_file -P ${spool_dir}/session_token"
  fi


  # ---> do it! (from here on we are changing things)
  publish_before_hook $name
  $user_shell "$dirtab_command" || { cvmfs_server_abort -f $name; die "Failed to apply .cvmfsdirtab"; }

  # check if we have open file descriptors on /cvmfs/<name>
  local use_fd_fallback=0
  handle_read_only_file_descriptors_on_mount_point $name $open_fd_dialog || use_fd_fallback=1

  publish_starting $name

  $user_shell "$ingest_command" || { publish_failed $name; cvmfs_server_abort -f $name; die "Synchronization failed\n\nExecuted Command:\n$ingest_command";   }

  cvmfs_sys_file_is_regular $manifest            || { publish_failed $name; cvmfs_server_abort -f $name; die "Manifest creation failed\n\nExecuted Command:\n$sync_command"; }

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
    cvmfs_sys_file_is_empty $manifest && { cvmfs_server_abort -f $name; die "failed to reload manifest"; }
  fi

  if [ x"$upstream_type" = xgw ]; then
      # TODO(jpriessn): implement publication counters upload to gateway
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
        $(get_swissknife_proxy)                             \
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
        cvmfs_server_abort -f $name; \
        die "Removing tags failed\n\nExecuted Command:\n \
        /bin/sh $tag_remove_cmd_file"; }
    rm -f $tag_remove_cmd_file
    # write intermediate history hash to reflog
    sign_manifest $name $manifest "" true
  fi

  # add a tag for the new revision
  echo "Tagging $name"
  $user_shell "$tag_command" || { publish_failed $name; cvmfs_server_abort -f $name; die "Tagging failed\n\nExecuted Command:\n$tag_command";  }

  if [ "x$sync_command_virtual_dir" != "x" ]; then
    # write intermediate catalog hash and history to reflog
    sign_manifest $name $manifest "" true
    $user_shell "$sync_command_virtual_dir" || { publish_failed $name; cvmfs_server_abort -f $name; die "Editing .cvmfs failed\n\nExecuted Command:\n$sync_command_virtual_dir";  }
    local trunk_hash=$(grep "^C" $manifest | tr -d C)
    $user_shell "$tag_command_undo_tags" || { publish_failed $name; cvmfs_server_abort -f $name; die "Creating undo tags\n\nExecuted Command:\n$tag_command_undo_tags";  }
  fi

  # finalizing transaction
  echo "Flushing file system buffers"
  sync

  # committing newly created revision
  echo "Signing new manifest"
  sign_manifest $name $manifest      || { publish_failed $name; cvmfs_server_abort -f $name; die "Signing failed"; }
  set_ro_root_hash $name $trunk_hash || { publish_failed $name; cvmfs_server_abort -f $name; die "Root hash update failed"; }
  if is_checked_out $name; then
    rm -f /var/spool/cvmfs/${name}/checkout
    echo "Reset to trunk on default branch"
  fi

  # run the automatic garbage collection (if configured)
  if is_due_auto_garbage_collection $name; then
    echo "Running automatic garbage collection"
    local dry_run=0
    __run_gc $name       \
             $stratum0   \
             $dry_run    \
             ""          \
             "0"         \
             -z $gc_timespan      || { local err=$?; publish_failed $name; cvmfs_server_abort -f $name; die "Garbage collection failed ($err)"; }
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
  if [ "x$CVMFS_UPLOAD_STATS_PLOTS" = "xtrue" ]; then
    /usr/share/cvmfs-server/upload_stats_plots.sh $name
  fi
  close_transaction $name $use_fd_fallback
  publish_after_hook $name
  publish_succeeded  $name

}
