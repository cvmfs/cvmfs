#
# This file is part of the CernVM File System
# This script takes care of creating, removing, and maintaining repositories
# on a Stratum 0/1 server
#
# Implementation of the "cvmfs_server diff" command

# This file depends on fuctions implemented in the following files:
# - cvmfs_server_util.sh
# - cvmfs_server_common.sh

cvmfs_server_checkout() {
  local name
  local branch_name
  local tag_name

  # optional parameter handling
  OPTIND=1
  while getopts "b:t:" option
  do
    case $option in
      b)
        branch_name="$OPTARG"
      ;;
      h)
        tag_name="$OPTARG"
      ;;
      ?)
        shift $(($OPTIND-2))
        usage "Command checkout: Unrecognized option: $1"
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

  # do it!
  local user_shell="$(get_user_shell $name)"
  local checkout_cmd="ls"
  $user_shell "$checkout_cmd"
}
