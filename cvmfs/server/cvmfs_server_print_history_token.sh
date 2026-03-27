#
# This file is part of the CernVM File System
# This script takes care of creating, removing, and maintaining repositories
# on a Stratum 0/1 server
#
# Implementation of the "cvmfs_server print-history-token" command

# This file depends on functions implemented in the following files:
# - cvmfs_server_util.sh
# - cvmfs_server_common.sh
# - cvmfs_server_apache.sh


cvmfs_server_print_history_token() {
  local name

  # get repository name
  OPTIND=1
  shift $(($OPTIND-1))
  check_parameter_count_with_guessing $#
  name=$(get_or_guess_repository_name $1)

  # sanity checks
  check_repository_existence $name || die "The repository $name does not exist"
  is_owner_or_root $name           || die "Permission denied: repository $name"

  load_repo_config $name

  if [ "x${CVMFS_PRIVATE_HISTORY}" != "xtrue" ]; then
    die "CVMFS_PRIVATE_HISTORY is not enabled for $name"
  fi

  print_history_token "$name"
}
