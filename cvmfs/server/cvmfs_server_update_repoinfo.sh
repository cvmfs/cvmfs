#
# This file is part of the CernVM File System
# This script takes care of creating, removing, and maintaining repositories
# on a Stratum 0/1 server
#
# Implementation of the "cvmfs_server update-repoinfo" command

# This file depends on functions implemented in the following files:
# - cvmfs_server_util.sh
# - cvmfs_server_common.sh

_update_repoinfo_cleanup() {
  local repo_name="$1"
  shift 1

  while [ $# -gt 0 ]; do
    rm -f $1 > /dev/null 2>&1
    shift 1
  done

  close_transaction $repo_name 0
}


cvmfs_server_update_repoinfo() {
  local name
  local json_file

  OPTIND=1
  while getopts "f:" option; do
    case $option in
      f)
        json_file=$OPTARG
      ;;
      ?)
        shift $(($OPTIND-2))
        usage "Command update-repoinfo: Unrecognized option: $1"
      ;;
    esac
  done
  shift $(($OPTIND-1))

  # get repository name
  check_parameter_count_with_guessing $#
  name=$(get_or_guess_repository_name $@)

  # sanity checks
  check_repository_existence $name || die "The repository $name does not exist"
  load_repo_config $name
  is_owner_or_root $name           || die "Permission denied: Repository $name is owned by $CVMFS_USER"
  is_stratum0 $name                || die "This is not a stratum 0 repository"
  ! is_publishing $name            || die "Repository is currently publishing"
  health_check -r $name
  is_in_transaction $name && die "Cannot edit repository meta info while in a transaction"
  [ x"$json_file" = x"" ] || [ -f "$json_file" ] || die "Provided file '$json_file' doesn't exist"

  tmp_file_info=$(mktemp)
  tmp_file_manifest=$(mktemp)
  chown $CVMFS_USER $tmp_file_info $tmp_file_manifest || die "Cannot change ownership of temporary files"

  trap "_update_repoinfo_cleanup $name $tmp_file_info $tmp_file_manifest" EXIT HUP INT TERM
  open_transaction $name || die "Failed to open transaction for meta info editing"

  get_repo_info -M > $tmp_file_info || \
    die "Failed getting repository meta info for $name"
  get_repo_info -R > $tmp_file_manifest || \
    die "Failed getting repository manifest for $name"

  if [ x"$json_file" = x"" ]; then
    if [ -f $tmp_file_info ] && [ ! -s $tmp_file_info ]; then
      create_repometa_skeleton $tmp_file_info
    fi

    edit_json_until_valid $tmp_file_info
  else
    local jq_output
    if ! jq_output="$(validate_json $json_file)"; then
      die "The provided JSON file is invalid. See below:\n${jq_output}"
    fi

    cat $json_file > $tmp_file_info
  fi

  sign_manifest $name $tmp_file_manifest $tmp_file_info
}
