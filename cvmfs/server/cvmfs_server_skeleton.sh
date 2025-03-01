#
# This file is part of the CernVM File System
# This script takes care of creating, removing, and maintaining repositories
# on a Stratum 0/1 server
#
# Implementation of the "cvmfs_server skeleton" command

# This file depends on functions implemented in the following files:
# - cvmfs_server_util.sh
# - cvmfs_server_common.sh


cvmfs_server_skeleton() {
  local skeleton_dir
  local skeleton_user

  # get optional parameters
  OPTIND=1
  while getopts "o:" option
  do
    case $option in
      o)
        skeleton_user=$OPTARG
        ;;
      ?)
        shift $(($OPTIND-2))
        usage "Command skeleton: Unrecognized option: $1"
      ;;
    esac
  done

  # get skeleton destination directory
  shift $(($OPTIND-1))

  # get skeleton destination directory
  if [ $# -eq 0 ]; then
    usage "Command skeleton: Please provide a skeleton destination directory"
  fi
  if [ $# -gt 1 ]; then
    usage "Command skeleton: Too many arguments"
  fi
  skeleton_dir=$1

  # ask for the skeleton dir owern
  if [ x$skeleton_user = "x" ]; then
    read -p "Owner of $skeleton_dir [$(whoami)]: " skeleton_user
    # default value
    [ x"$skeleton_user" = x ] && skeleton_user=$(whoami)
  fi

  # sanity checks
  check_user $skeleton_user || die "No user $skeleton_user"

  # do it!
  create_repository_skeleton $skeleton_dir $skeleton_user
}


