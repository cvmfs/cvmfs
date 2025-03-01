#
# This file is part of the CernVM File System
# This script takes care of creating, removing, and maintaining repositories
# on a Stratum 0/1 server
#
# Implementation of the "cvmfs_server update-info" command

# This file depends on functions implemented in the following files:
# - cvmfs_server_util.sh
# - cvmfs_server_common.sh


_update_info_cleanup() {
  local tmp_file="$1"
  rm -f $tmp_file > /dev/null 2>&1
}

cvmfs_server_update_info() {
  local configure_apache=1
  local edit_meta_info=1

  # parameter handling
  OPTIND=1
  while getopts "pe" option; do
    case $option in
      p)
        configure_apache=0
      ;;
      e)
        edit_meta_info=0
      ;;
      ?)
        shift $(($OPTIND-2))
        usage "Command update-info: Unrecognized option: $1"
      ;;
    esac
  done
  shift $(($OPTIND-1))

  # sanity checks
  is_root || die "only root can update meta information"

  # create info HTTP resource if not existent yet
  if ! has_global_info_path; then
    echo -n "Creating Info Resource... "
    create_global_info_skeleton || die "fail"
    echo "done"
  fi

  if [ $configure_apache -eq 1 ] && ! has_apache_config_for_global_info; then
    echo -n "Creating Apache Configuration for Info Resource... "
    create_apache_config_for_global_info || die "fail (create apache config)"
    reload_apache > /dev/null            || die "fail (reload apache)"
    echo "done"
  fi

  # manually edit the meta information file
  local tmp_file=""
  if [ $edit_meta_info -eq 1 ]; then
    # copy the meta information file for editing
    tmp_file=$(mktemp)
    trap "_update_info_cleanup $tmp_file" EXIT HUP INT TERM
    cp -f "$(get_global_info_v1_path)/meta.json" $tmp_file

    edit_json_until_valid $tmp_file || die "Aborting..."
  fi

  # update the JSON files
  echo -n "Updating global JSON information... "
  update_global_repository_info || die "fail (update repo info)"
  if [ $edit_meta_info -eq 1 ]; then
    update_global_meta_info "$tmp_file" || die "fail (update meta info)"
  fi
  echo "done"
}


