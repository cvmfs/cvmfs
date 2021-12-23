#
# This file is part of the CernVM File System
# This script takes care of creating, removing, and maintaining repositories
# on a Stratum 0/1 server
#
# Implementation of the "cvmfs_server eliminate-hardlinks" command

# This file depends on functions implemented in the following files:
# - cvmfs_server_util.sh
# - cvmfs_server_common.sh


cvmfs_server_eliminate_hardlinks() {
  local name=
  local force=0

  # parameter handling
  OPTIND=1
  while getopts "f" option; do
    case $option in
      f)
        force=1
      ;;
      ?)
        shift $(($OPTIND-2))
        usage "Command eliminate-hardlinks: Unrecognized option: $1"
      ;;
    esac
  done
  shift $(($OPTIND-1))

  # get repository name
  check_parameter_count_with_guessing $#
  name=$(get_or_guess_repository_name $@)
  check_repository_existence "$name"

  load_repo_config $name

  is_root || die "Permission denied: Only root can do that"
  is_checked_out $name && die "command is not supported while checked out onto a branch"

  if [ $force -ne 1 ]; then
    echo "This will break up all hardlink relationships that are currently"
    echo "present in '$name'. This process cannot be undone!"
    echo ""
    echo -n "Are you sure? (y/N): "

    local reply="n"
    read reply
    if [ "$reply" != "y" ] && [ "$reply" != "Y" ]; then
      echo "aborted."
      exit 1
    fi
  fi

  local migrate_command="$(__swissknife_cmd dbg) migrate     \
                              -v 'hardlink'                  \
                              -r $CVMFS_STRATUM0             \
                              -n $name                       \
                              -u $CVMFS_UPSTREAM_STORAGE     \
                              -k $CVMFS_PUBLIC_KEY           \
                              -s"

  _run_catalog_migration "$name" "$migrate_command"
}


