#
# This file is part of the CernVM File System
# This script takes care of creating, removing, and maintaining repositories
# on a Stratum 0/1 server
#
# Implementation of the "cvmfs_server chown" command

# This file depends on functions implemented in the following files:
# - cvmfs_server_util.sh
# - cvmfs_server_common.sh


cvmfs_server_catalog_chown() {
  local uid_map
  local gid_map

  OPTIND=1
  while getopts "u:g:" option; do
    case $option in
      u)
        uid_map=$OPTARG
      ;;
      g)
        gid_map=$OPTARG
      ;;
      ?)
        shift $(($OPTIND-2))
        usage "Command catalog-chown: Unrecognized option: $1"
      ;;
    esac
  done
  shift $(($OPTIND-1))

   # get repository names
  check_parameter_count_with_guessing $#
  name=$(get_or_guess_repository_name $@)
  check_repository_existence "$name"

  # sanity checks
  [ x"$uid_map" != x"" ] && cvmfs_sys_file_is_regular $uid_map || die "UID map file not found (-u)"
  [ x"$gid_map" != x"" ] && cvmfs_sys_file_is_regular $gid_map || die "GID map file not found (-g)"

  load_repo_config $name
  is_checked_out $name && die "command is not supported while checked out onto a branch"

  local migrate_command="$(__swissknife_cmd dbg) migrate     \
                              -v 'chown'                     \
                              -r $CVMFS_STRATUM0             \
                              -n $name                       \
                              -u $CVMFS_UPSTREAM_STORAGE     \
                              -k $CVMFS_PUBLIC_KEY           \
                              -i $uid_map                    \
                              -j $gid_map                    \
                              -s"

  _run_catalog_migration "$name" "$migrate_command"
}


