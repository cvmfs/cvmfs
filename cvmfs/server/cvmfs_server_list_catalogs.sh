#
# This file is part of the CernVM File System
# This script takes care of creating, removing, and maintaining repositories
# on a Stratum 0/1 server
#
# Implementation of the "cvmfs_server list-catalogs" command

# This file depends on functions implemented in the following files:
# - cvmfs_server_util.sh
# - cvmfs_server_common.sh

cvmfs_server_list_catalogs() {
  local name
  local param_list="-t"

  # optional parameter handling
  OPTIND=1
  while getopts "sehx" option
  do
    case $option in
      s)
        param_list="$param_list -s"
      ;;
      e)
        param_list="$param_list -e"
      ;;
      h)
        param_list="$param_list -d"
      ;;
      x)
        param_list=$(echo "$param_list" | sed 's/-t\s\?//')
      ;;
      ?)
        shift $(($OPTIND-2))
        usage "Command list-catalogs: Unrecognized option: $1"
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
  health_check     $name || die "Repository $name is not healthy"

  # check if repository is compatible to the installed CernVM-FS version
  check_repository_compatibility $name

  if is_checked_out $name; then
    param_list="$param_list -h $(get_checked_out_hash $name)"
  fi

  # do it!
  local user_shell="$(get_user_shell $name)"
  local lsrepo_cmd
  lsrepo_cmd="$(__swissknife_cmd dbg) lsrepo     \
                       -r $CVMFS_STRATUM0        \
                       -n $CVMFS_REPOSITORY_NAME \
                       -k $CVMFS_PUBLIC_KEY      \
                       -l ${CVMFS_SPOOL_DIR}/tmp \
                       $param_list"
  $user_shell "$lsrepo_cmd"
}


