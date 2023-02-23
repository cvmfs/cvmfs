#
# This file is part of the CernVM File System
# This script takes care of creating, removing, and maintaining repositories
# on a Stratum 0/1 server
#
# Implementation of the "cvmfs_server mount" command

# This file depends on functions implemented in the following files:
# - cvmfs_server_util.sh
# - cvmfs_server_common.sh


cvmfs_server_mount() {
  local names=""
  local mount_all=0
  local retval=0

  OPTIND=1
  while getopts "a" option; do
    case $option in
      a)
        mount_all=1
      ;;
      ?)
        shift $(($OPTIND-2))
        usage "Command mount: Unrecognized option: $1"
      ;;
    esac
  done
  shift $(($OPTIND-1))

  if [ $mount_all -eq 1 ]; then
    # sanity checks
    is_root || die "Permission Denied: need root to mount all repositories"
    names="$(ls /etc/cvmfs/repositories.d)"
  else
    # get repository name
    check_parameter_count_for_multiple_repositories $#
    names=$(get_or_guess_multiple_repository_names "$@")
    check_multiple_repository_existence "$names"
  fi

  for name in $names; do
    is_stratum0        $name || continue
    is_owner_or_root   $name || { echo "Permission Denied: $name is owned by $CVMFS_USER" >&2; retval=1; continue; }
    health_check -rftq $name || { echo "Failed to mount $name"                            >&2; retval=1; continue; }
  done

  return $retval
}


