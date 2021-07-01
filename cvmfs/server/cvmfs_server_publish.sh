cvmfs_server_publish() {
  local names
  local user
  local gw_key_file
  local spool_dir
  local stratum0
  local upstream
  local hash_algorithm
  local tweaks_option=
  local tag_name=
  local tag_channel=00
  local tag_description=
  local retcode=0
  local verbosity=""
  local manual_revision=""
  local gc_timespan=0
  local authz_file=""
  local force_external=0
  local force_native=0
  local force_direct_io=0
  local force_compression_algorithm=""
  local external_option=""
  local direct_io_option=""
  local open_fd_dialog=1

  # optional parameter handling
  OPTIND=1
  while getopts "F:NXZ:pa:c:m:vn:fed" option
  do
    case $option in
      p)
        tweaks_option="-d"
      ;;
      a)
        tag_name="$OPTARG"
      ;;
      c)
        tag_channel="$OPTARG"
      ;;
      m)
        tag_description="$OPTARG"
      ;;
      v)
        verbosity="-x"
      ;;
      n)
        manual_revision="$OPTARG"
      ;;
      X)
        force_external=1
      ;;
      N)
        force_native=1
      ;;
      Z)
        force_compression_algorithm="$OPTARG"
      ;;
      F)
        authz_file="-F $OPTARG"
      ;;
      d)
        force_direct_io=1
      ;;
      f)
        open_fd_dialog=0
      ;;
      ?)
        shift $(($OPTIND-2))
        usage "Command publish: Unrecognized option: $1"
      ;;
    esac
  done

  if [ $(($force_external + $force_native)) -eq 2 ]; then
    usage "Command publish: -N and -X are mutually exclusive"
  fi

  shift $(($OPTIND-1))
  check_parameter_count_for_multiple_repositories $#
  # get repository names
  names=$(get_or_guess_multiple_repository_names "$@")
  check_multiple_repository_existence "$names"

  for name in $names; do
    # sanity checks
    if [ ! -z "$tag_name" ]; then
      echo $tag_name | grep -q -v " "       || die "Spaces are not allowed in tag names"
      check_tag_existence $name "$tag_name" && die "Tag name '$tag_name' is already in use."
    fi

    # Ignore any subpath appended to the repository e.g. repo.cern.ch/sub/path/for/locking
    # Providing a subpath for the "cvmfs_server publish" command is no longer needed.
    name=$(echo $name | cut -d'/' -f1)

    load_repo_config $name
    # We need the upstream type for configuring the health_check function
    upstream=$CVMFS_UPSTREAM_STORAGE
    upstream_type=$(get_upstream_type $upstream)

    # sanity checks
    is_stratum0 $name   || die "This is not a stratum 0 repository"
    is_publishing $name && die "Another publish process is active for $name"
    if [ x"$upstream_type" = xgw ]; then
        health_check -g -r $name
    else
        # TODO(jblomer): switch me back to `health_check -r $name`
        health_check -g -r $name
    fi

    # get repository information
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
    if [ $force_direct_io -eq 1 ]; then
      direct_io_option="-W"
    fi

    # more sanity checks
    is_owner_or_root $name || { echo "Permission denied: Repository $name is owned by $user"; retcode=1; continue; }
    check_repository_compatibility $name
    check_url "${CVMFS_STRATUM0}/.cvmfspublished" 20 || { echo "Repository unavailable under $CVMFS_STRATUM0"; retcode=1; continue; }
    check_expiry $name $stratum0   || { echo "Repository whitelist for $name is expired!"; retcode=1; continue; }
    is_in_transaction $name        || { echo "Repository $name is not in a transaction"; retcode=1; continue; }
    [ $(count_wr_fds /cvmfs/$name) -eq 0 ] || { echo "Open writable file descriptors on $name"; retcode=1; continue; }
    is_cwd_on_path "/cvmfs/$name" && { echo "Current working directory is in /cvmfs/$name.  Please release, e.g. by 'cd \$HOME'."; retcode=1; continue; } || true
    gc_timespan="$(get_auto_garbage_collection_timespan $name)" || { retcode=1; continue; }
    if [ x"$manual_revision" != x"" ]; then
      if [ "x$(echo "$manual_revision" | tr -cd 0-9)" != "x$manual_revision" ]; then
        echo "Invalid revision number: $manual_revision"
        retcode=1
        continue
      fi
      local revision_number=$(attr -qg revision /var/spool/cvmfs/${name}/rdonly)
      if [ $manual_revision -le $revision_number ]; then
        echo "Current revision '$revision_number' is ahead of manual revision number '$manual_revision'."
        retcode=1
        continue
      fi
    fi

    if is_checked_out $name; then
      if [ x"$tag_name" = "x" ]; then
        echo "Publishing a checked out revision requires a tag name"
        retcode=1
        continue
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
      auto_tag_cleanup_list="$(filter_auto_tags $name)" || { echo "failed to determine outdated auto tags on $name"; retcode=1; continue; }
    fi

    # prepare the commands to be used for the publishing later
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
    local sync_command="$(__swissknife_cmd dbg) sync \
        -u /cvmfs/$name                                \
        -s ${scratch_dir}                              \
        -c ${spool_dir}/rdonly                         \
        -t ${spool_dir}/tmp                            \
        -b $base_hash                                  \
        -r ${upstream}                                 \
        -w $stratum0                                   \
        -o $manifest                                   \
        -e $hash_algorithm                             \
        -Z $compression_alg                            \
        -C $trusted_certs                              \
        -N $name                                       \
        -K $CVMFS_PUBLIC_KEY                           \
        $(get_follow_http_redirects_flag)              \
        $authz_file                                    \
        $log_level $tweaks_option $external_option $direct_io_option $verbosity"

    if [ ! -z "$tag_name" ]; then
      sync_command="$sync_command -D $tag_name"
    fi
    if [ ! -z "$tag_channel" ]; then
      sync_command="$sync_command -G $tag_channel"
    fi

    if [ x"$tag_description" != x"" ]; then
      sync_command="$sync_command -J $tag_description"
    fi

    # If the upstream type is "gw", we need to additionally pass
    # the names of the file containing the gateway key and of the
    # one containing the session token
    if [ x"$upstream_type" = xgw ]; then
      sync_command="$sync_command -H $gw_key_file -P ${spool_dir}/session_token"
    fi
    if [ "x$CVMFS_UNION_FS_TYPE" != "x" ]; then
      sync_command="$sync_command -f $CVMFS_UNION_FS_TYPE"
    fi
    if [ "x${CVMFS_GENERATE_LEGACY_BULK_CHUNKS:-$CVMFS_DEFAULT_GENERATE_LEGACY_BULK_CHUNKS}" = "xtrue" ]; then
      sync_command="$sync_command -O"
    fi
    if [ "x$CVMFS_USE_FILE_CHUNKING" = "xtrue" ]; then
      sync_command="$sync_command -p \
       -l $CVMFS_MIN_CHUNK_SIZE \
       -a $CVMFS_AVG_CHUNK_SIZE \
       -h $CVMFS_MAX_CHUNK_SIZE"
    fi
    if [ "x$CVMFS_AUTOCATALOGS" = "xtrue" ]; then
      sync_command="$sync_command -A"
    fi
    if [ "x$CVMFS_AUTOCATALOGS_MAX_WEIGHT" != "x" ]; then
      sync_command="$sync_command -X $CVMFS_AUTOCATALOGS_MAX_WEIGHT"
    fi
    if [ "x$CVMFS_AUTOCATALOGS_MIN_WEIGHT" != "x" ]; then
      sync_command="$sync_command -M $CVMFS_AUTOCATALOGS_MIN_WEIGHT"
    fi
    if [ "x$CVMFS_IGNORE_XDIR_HARDLINKS" = "xtrue" ]; then
      sync_command="$sync_command -i"
    fi
    if [ "x$CVMFS_INCLUDE_XATTRS" = "xtrue" ]; then
      sync_command="$sync_command -k"
    fi
    if [ "x${CVMFS_ENFORCE_LIMITS:-$CVMFS_DEFAULT_ENFORCE_LIMITS}" = "xtrue" ]; then
      sync_command="$sync_command -E"
    fi
    if [ "x$CVMFS_NESTED_KCATALOG_LIMIT" != "x" ]; then
      sync_command="$sync_command -Q $CVMFS_NESTED_KCATALOG_LIMIT"
    fi
    if [ "x$CVMFS_ROOT_KCATALOG_LIMIT" != "x" ]; then
      sync_command="$sync_command -R $CVMFS_ROOT_KCATALOG_LIMIT"
    fi
    if [ "x$CVMFS_FILE_MBYTE_LIMIT" != "x" ]; then
      sync_command="$sync_command -U $CVMFS_FILE_MBYTE_LIMIT"
    fi
    if [ "x$CVMFS_NUM_UPLOAD_TASKS" != "x" ]; then
      sync_command="$sync_command -0 $CVMFS_NUM_UPLOAD_TASKS"
    fi
    if [ "x$manual_revision" != "x" ]; then
      sync_command="$sync_command -v $manual_revision"
    fi
    if [ "x$CVMFS_REPOSITORY_TTL" != "x" ]; then
      sync_command="$sync_command -T $CVMFS_REPOSITORY_TTL"
    fi
    if [ "x$CVMFS_MAXIMAL_CONCURRENT_WRITES" != "x" ]; then
      sync_command="$sync_command -q $CVMFS_MAXIMAL_CONCURRENT_WRITES"
    fi
    if [ "x${CVMFS_VOMS_AUTHZ}" != x ]; then
      sync_command="$sync_command -V"
    fi
    if [ "x$CVMFS_IGNORE_SPECIAL_FILES" = "xtrue" ]; then
      sync_command="$sync_command -g"
    fi
    if [ "x$CVMFS_UPLOAD_STATS_DB" = "xtrue" ]; then
      sync_command="$sync_command -I"
    fi
    local sync_command_virtual_dir=
    if [ "x${CVMFS_VIRTUAL_DIR}" = "xtrue" ]; then
      sync_command_virtual_dir="$sync_command -S snapshots"
    else
      if [ -d /cvmfs/$name/.cvmfs ]; then
        sync_command_virtual_dir="$sync_command -S remove"
      fi
    fi
    if [ "x$CVMFS_PRINT_STATISTICS" = "xtrue" ]; then
      sync_command="$sync_command -+stats"
    fi
    # Must be after the virtual-dir command is constructed
    if is_checked_out $name; then
      sync_command="$sync_command -B"
    fi

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

    # ---> do it! (from here on we are changing things)
    publish_before_hook $name
    $user_shell "$dirtab_command" || die "Failed to apply .cvmfsdirtab"

    # check if we have open file descriptors on /cvmfs/<name>
    local use_fd_fallback=0
    handle_read_only_file_descriptors_on_mount_point $name $open_fd_dialog || use_fd_fallback=1

    # synchronize the repository
    publish_starting $name
    $user_shell "$sync_command" || { publish_failed $name; die "Synchronization failed\n\nExecuted Command:\n$sync_command";   }
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
    syncfs

    # committing newly created revision
    echo "Signing new manifest"
    sign_manifest $name $manifest      || { publish_failed $name; die "Signing failed"; }
    set_ro_root_hash $name $trunk_hash || { publish_failed $name; die "Root hash update failed"; }
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
    if [ "x$CVMFS_UPLOAD_STATS_PLOTS" = "xtrue" ]; then
      /usr/share/cvmfs-server/upload_stats_plots.sh $name
    fi
    close_transaction  $name $use_fd_fallback
    publish_after_hook $name
    publish_succeeded  $name
    syncfs
  done

  return $retcode
}


has_file_descriptors_on_mount_point() {
  local name=$1
  local mountpoint="/cvmfs/${name}"

  [ $(count_rd_only_fds $mountpoint) -gt 0 ] || \
  [ $(count_wr_fds      $mountpoint) -gt 0 ]
}


# Lists all auto-generated tags
#
# @param repository_name   the name of the repository to be filtered
# @return                  list of outdated auto-generate tags, space-separated
#              Note: in case of a errors it might print an error to stderr and
#              return a non-zero code
filter_auto_tags() {
  local repository_name="$1"
  local auto_tags_timespan=
  auto_tags_timespan=$(get_auto_tags_timespan "$repository_name") || return 1
  [ $auto_tags_timespan -eq 0 ] && return 0 || true

  load_repo_config $repository_name
  local auto_tags="$(__swissknife tag_list      \
    -w $CVMFS_STRATUM0                         \
    -t ${CVMFS_SPOOL_DIR}/tmp                  \
    -p /etc/cvmfs/keys/${repository_name}.pub  \
    -f $repository_name                        \
    -x $(get_follow_http_redirects_flag)       | \
    grep -E \
    '^generic(_[[:digit:]]+)?-[[:digit:]]{4}-[[:digit:]]{2}-[[:digit:]]{2}T[[:digit:]]{2}:[[:digit:]]{2}:[[:digit:]]{2}Z' | \
    awk '{print $1 " " $5}')"
  [ "x$auto_tags" = "x" ] && return 0 || true

  local tag_name=
  local timestamp=
  local old_tags="$(echo "$auto_tags" | while read tag_name timestamp; do
    if [ "$timestamp" -lt "$auto_tags_timespan" ]; then
      echo -n "$tag_name "
    fi
  done)"
  # Trim old_tags
  echo $old_tags
}


publish_starting() {
  local name=$1
  load_repo_config $name
  local pub_lock="${CVMFS_SPOOL_DIR}/is_publishing"
  acquire_lock "$pub_lock" || die "Failed to acquire publishing lock"
  trap "publish_failed $name" EXIT HUP INT TERM
  run_suid_helper lock $name
  to_syslog_for_repo $name "started publishing"
}


publish_failed() {
  local name=$1
  load_repo_config $name
  local pub_lock="${CVMFS_SPOOL_DIR}/is_publishing"
  trap - EXIT HUP INT TERM
  release_lock $pub_lock
  run_suid_helper open $name
  to_syslog_for_repo $name "failed to publish"
}


publish_succeeded() {
  local name=$1
  load_repo_config $name
  local pub_lock="${CVMFS_SPOOL_DIR}/is_publishing"
  trap - EXIT HUP INT TERM
  release_lock $pub_lock
  to_syslog_for_repo $name "successfully published revision $(get_repo_info -v)"
}

