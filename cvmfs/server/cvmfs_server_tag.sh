#
# This file is part of the CernVM File System
# This script takes care of creating, removing, and maintaining repositories
# on a Stratum 0/1 server
#
# Implementation of the "cvmfs_server tag" command

# This file depends on functions implemented in the following files:
# - cvmfs_server_util.sh
# - cvmfs_server_common.sh

cvmfs_server_tag() {
  local name
  local tag_name=""
  local action_add=0
  local add_tag_channel
  local add_tag_description
  local add_tag_root_hash
  local action_remove=0
  local tag_names=""
  local remove_tag_force=0
  local action_inspect=0
  local action_list_tags=0
  local action_list_branches=0
  local machine_readable=0
  local silence_warnings=0

  # optional parameter handling
  OPTIND=1
  while getopts "a:c:m:h:r:fblxi:" option
  do
    case $option in
      a)
        tag_name="$OPTARG"
        action_add=1
      ;;
      c)
        add_tag_channel=$OPTARG
        ;;
      m)
        add_tag_description="$OPTARG"
        ;;
      h)
        add_tag_root_hash=$OPTARG
        ;;
      r)
        [ -z "$tag_names" ]      \
          && tag_names="$OPTARG" \
          || tag_names="$tag_names $OPTARG"
        action_remove=1
        ;;
      f)
        remove_tag_force=1
        ;;
      l)
        action_list_tags=1
        ;;
      b)
        action_list_branches=1
        ;;
      x)
        machine_readable=1
        silence_warnings=1
        ;;
      i)
        tag_name="$OPTARG"
        action_inspect=1
        ;;
      ?)
        shift $(($OPTIND-2))
        usage "Command tag: Unrecognized option: $1"
      ;;
    esac
  done

  # get repository name
  shift $(($OPTIND-1))
  check_parameter_count_with_guessing $#
  name=$(get_or_guess_repository_name $1)

  # check for ambiguous action requests
  local actions=$(( $action_remove+$action_list_tags+$action_list_branches+$action_add+$action_inspect ))
  [ $actions -gt 0 ] || { action_list_tags=1; actions=$(( $actions + 1 )); } # listing is the default action
  [ $actions -eq 1 ] || die "Ambiguous parameters. Please either add, remove, inspect or list tags."

  # sanity checks
  check_repository_existence $name || die "The repository $name does not exist"
  load_repo_config $name
  is_owner_or_root $name           || die "Permission denied: Repository $name is owned by $CVMFS_USER"
  is_stratum0 $name                || die "This is not a stratum 0 repository"
  ! is_publishing $name            || die "Repository is currently publishing"
  health_check -r $name

  local base_hash="$(get_mounted_root_hash $name)"
  local user_shell="$(get_user_shell $name)"
  local hash_algorithm="${CVMFS_HASH_ALGORITHM-sha1}"

  # listing does not need an open repository transaction
  if [ $action_list_tags -eq 1 ] || [ $action_list_branches -eq 1 ] || [ $actions -eq 0 ]; then
    local tag_list_command="$(__swissknife_cmd dbg) tag_list \
      -w $CVMFS_STRATUM0                                     \
      -t ${CVMFS_SPOOL_DIR}/tmp                              \
      -p /etc/cvmfs/keys/${name}.pub                         \
      -z /etc/cvmfs/repositories.d/${name}/trusted_certs     \
      -f $name"
    if [ $machine_readable -ne 0 ]; then
      tag_list_command="$tag_list_command -x"
    fi
    if [ $action_list_branches -eq 1 ]; then
      tag_list_command="$tag_list_command -B"
    fi
    $user_shell "$tag_list_command"
    return $?
  fi

  # tag inspection does not need to open a repository transaction
  if [ $action_inspect -eq 1 ]; then
    local tag_inspect_command="$(__swissknife_cmd dbg) tag_info \
      -w $CVMFS_STRATUM0                                        \
      -t ${CVMFS_SPOOL_DIR}/tmp                                 \
      -p /etc/cvmfs/keys/${name}.pub                            \
      -z /etc/cvmfs/repositories.d/${name}/trusted_certs        \
      -f $name                                                  \
      -n $tag_name"
    if [ $machine_readable -ne 0 ]; then
      tag_inspect_command="$tag_inspect_command -x"
    fi
    $user_shell "$tag_inspect_command"
    return $?
  fi

  # all following commands need an open repository transaction and are supposed
  # to commit or abort it after performing a tag database manipulation. Hence,
  # they also need to performed by the repository owner or root
  [ ! -z "$tag_name" -o ! -z "$tag_names" ] || die "Tag name missing"
  echo "$tag_name" | grep -q -v " "         || die "Spaces are not allowed in tag names"

  is_checked_out $name && die "Cannot modify tags when checked out on another branch"
  is_in_transaction $name && die "Cannot change repository tags while in a transaction"
  trap "close_transaction $name 0" EXIT HUP INT TERM
  open_transaction $name || die "Failed to open transaction for tag manipulation"

  local log_level=
  [ "x$CVMFS_LOG_LEVEL" != x ] && log_level="-z $CVMFS_LOG_LEVEL"
  local new_manifest="${CVMFS_SPOOL_DIR}/tmp/manifest"
  local sync_command_virtual_dir="$(__swissknife_cmd dbg) sync \
      -u /cvmfs/$name                                    \
      -s ${CVMFS_SPOOL_DIR}/scratch/current              \
      -c ${CVMFS_SPOOL_DIR}/rdonly                       \
      -t ${CVMFS_SPOOL_DIR}/tmp                          \
      -b $base_hash                                      \
      -r $CVMFS_UPSTREAM_STORAGE                         \
      -w $CVMFS_STRATUM0                                 \
      -o ${new_manifest}~                                \
      -e $hash_algorithm                                 \
      -Z ${CVMFS_COMPRESSION_ALGORITHM-default}          \
      -C /etc/cvmfs/repositories.d/${name}/trusted_certs \
      -N $name                                           \
      -K $CVMFS_PUBLIC_KEY                               \
      $(get_follow_http_redirects_flag) $log_level -S snapshots"
  local tag_command_undo_tags="$(__swissknife_cmd dbg) tag_edit \
      -r $CVMFS_UPSTREAM_STORAGE                        \
      -w $CVMFS_STRATUM0                                \
      -t ${CVMFS_SPOOL_DIR}/tmp                         \
      -m ${new_manifest}~                               \
      -p /etc/cvmfs/keys/${name}.pub                    \
      -f $name                                          \
      -e $hash_algorithm                                \
      $(get_follow_http_redirects_flag)                 \
      -x"

  # adds (or moves) a tag in the database
  if [ $action_add -eq 1 ]; then
    local tag_create_command="$(__swissknife_cmd dbg) tag_edit   \
      -w $CVMFS_STRATUM0                                         \
      -t ${CVMFS_SPOOL_DIR}/tmp                                  \
      -p /etc/cvmfs/keys/${name}.pub                             \
      -z /etc/cvmfs/repositories.d/${name}/trusted_certs         \
      -f $name                                                   \
      -r $CVMFS_UPSTREAM_STORAGE                                 \
      -m $new_manifest                                           \
      -b $base_hash                                              \
      -e $hash_algorithm                                         \
      $(get_follow_http_redirects_flag)                          \
      -a $tag_name"
    if [ ! -z "$add_tag_channel" ]; then
      tag_create_command="$tag_create_command -c $add_tag_channel"
    fi
    if [ ! -z "$add_tag_description" ]; then
      tag_create_command="$tag_create_command -D \"$add_tag_description\""
    fi
    if [ ! -z "$add_tag_root_hash" ]; then
      tag_create_command="$tag_create_command -h $add_tag_root_hash"
    fi
    $user_shell "$tag_create_command" || exit 1
    cp "$new_manifest" "${new_manifest}~"
    sign_manifest $name $new_manifest || die "Failed to sign repo"
  fi

  # removes one or more tags from the database
  if [ $action_remove -eq 1 ]; then
    if [ $remove_tag_force -eq 0 ]; then
      echo "You are about to remove these tags from $name:"
      for t in $tag_names; do echo "* $t"; done
      echo
      local reply
      read -p "Are you sure (y/N)? " reply
      if [ "$reply" != "y" ] && [ "$reply" != "Y" ]; then
        return 1
      fi
    fi

    $user_shell "$(__swissknife_cmd dbg) tag_edit        \
      -w $CVMFS_STRATUM0                                 \
      -t ${CVMFS_SPOOL_DIR}/tmp                          \
      -p /etc/cvmfs/keys/${name}.pub                     \
      -z /etc/cvmfs/repositories.d/${name}/trusted_certs \
      -f $name                                           \
      -r $CVMFS_UPSTREAM_STORAGE                         \
      -m $new_manifest                                   \
      -b $base_hash                                      \
      -e $hash_algorithm                                 \
      -d '$tag_names'" || die "Did not remove anything"
    cp "$new_manifest" "${new_manifest}~"
    sign_manifest $name $new_manifest || die "Failed to sign repo"
  fi

  if [ "x${CVMFS_VIRTUAL_DIR}" = "xtrue" ]; then
    $user_shell "$sync_command_virtual_dir" || die "Failed to create virtual catalog"
    local trunk_hash=$(grep "^C" ${new_manifest}~ | tr -d C)
    tag_command_undo_tags="$tag_command_undo_tags -b $trunk_hash"
    $user_shell "$tag_command_undo_tags" || die "Failed to set trunk hash"
    sign_manifest $name ${new_manifest}~ || die "Failed to sign repo"
    set_ro_root_hash $name $trunk_hash   || die "Root hash update failed"
  fi
  rm -f ${new_manifest}~
}


