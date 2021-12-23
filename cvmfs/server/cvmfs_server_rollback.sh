#
# This file is part of the CernVM File System
# This script takes care of creating, removing, and maintaining repositories
# on a Stratum 0/1 server
#
# Implementation of the "cvmfs_server rollback" command

# This file depends on functions implemented in the following files:
# - cvmfs_server_util.sh
# - cvmfs_server_common.sh
# - cvmfs_server_ssl.sh


cvmfs_server_rollback() {
  local name
  local user
  local spool_dir
  local stratum0
  local upstream
  local target_tag=""
  local undo_rollback=1
  local force=0

  # optional parameter handling
  OPTIND=1
  while getopts "t:f" option
  do
    case $option in
      t)
        target_tag=$OPTARG
        undo_rollback=0
      ;;
      f)
        force=1
      ;;
      ?)
        shift $(($OPTIND-2))
        usage "Command rollback: Unrecognized option: $1"
      ;;
    esac
  done

  # get repository name
  shift $(($OPTIND-1))
  check_parameter_count_with_guessing $#
  name=$(get_or_guess_repository_name $1)

  # sanity checks
  check_repository_existence $name || die "The repository $name does not exist"
  is_stratum0 $name                || die "This is not a stratum 0 repository"
  is_publishing $name              && die "Repository $name is currently being published"
  is_checked_out $name             && die "Can't rollback when checked out on a branch"
  health_check -r $name

  # get repository information
  load_repo_config $name
  user=$CVMFS_USER
  spool_dir=$CVMFS_SPOOL_DIR
  stratum0=$CVMFS_STRATUM0
  upstream=$CVMFS_UPSTREAM_STORAGE

  # more sanity checks
  is_owner_or_root $name || die "Permission denied: Repository $name is owned by $user"
  check_repository_compatibility $name
  check_url "${CVMFS_STRATUM0}/.cvmfspublished" 20 || die "Repository unavailable under $CVMFS_STRATUM0"
  check_expiry $name $stratum0  || die "Repository whitelist is expired!"
  is_in_transaction $name && die "Cannot rollback a repository in a transaction"
  is_cwd_on_path "/cvmfs/$name" && die "Current working directory is in /cvmfs/$name.  Please release, e.g. by 'cd \$HOME'." || true

  if [ $undo_rollback -eq 1 ]; then
    if ! check_tag_existence $name "trunk-previous"; then
      die "More than one anonymous undo rollback is not supported. Please specify a tag name (-t)"
    fi
  elif ! check_tag_existence $name "$target_tag"; then
    die "Target tag '$target_tag' does not exist"
  fi

  if [ $force -ne 1 ]; then
    local reply
    if [ $undo_rollback -eq 1 ]; then
      read -p "You are about to UNDO your last published revision!  Are you sure (y/N)? " reply
    else
      read -p "You are about to ROLLBACK to $target_tag AS THE LATEST REVISION!  Are you sure (y/N)? " reply
    fi
    if [ "$reply" != "y" ] && [ "$reply" != "Y" ]; then
      return 1
    fi
  fi

  # prepare the shell commands
  local user_shell="$(get_user_shell $name)"
  local base_hash=$(get_mounted_root_hash $name)
  local hash_algorithm="${CVMFS_HASH_ALGORITHM-sha1}"

  local rollback_command="$(__swissknife_cmd dbg) tag_rollback \
    -w $stratum0                                               \
    -t ${spool_dir}/tmp                                        \
    -p /etc/cvmfs/keys/${name}.pub                             \
    -z /etc/cvmfs/repositories.d/${name}/trusted_certs         \
    -f $name                                                   \
    -r $upstream                                               \
    -m ${spool_dir}/tmp/manifest                               \
    -b $base_hash                                              \
    -e $hash_algorithm"
  if [ ! -z "$target_tag" ]; then
    rollback_command="$rollback_command -n $target_tag"
  fi

  # do it!
  echo "Rolling back repository (leaving behind $base_hash)"
  trap "close_transaction $name 0" EXIT HUP INT TERM
  open_transaction $name || die "Failed to open transaction for rollback"

  $user_shell "$rollback_command" || die "Rollback failed\n\nExecuted Command:\n$rollback_command";

  local trunk_hash=$(grep "^C" ${spool_dir}/tmp/manifest | tr -d C)
  sign_manifest $name ${spool_dir}/tmp/manifest || die "Signing failed";
  set_ro_root_hash $name $trunk_hash

  echo "Flushing file system buffers"
  syncfs
}


