#
# This file is part of the CernVM File System
# This script takes care of creating, removing, and maintaining repositories
# on a Stratum 0/1 server
#
# Implementation of the "cvmfs_server rmfs" command

# This file depends on functions implemented in the following files:
# - cvmfs_server_util.sh
# - cvmfs_server_common.sh

cvmfs_server_rmfs() {
  local names
  local force=0
  local preserve_data=0
  local retcode=0

  # optional parameter handling
  OPTIND=1
  while getopts "fp" option
  do
    case $option in
      f)
        force=1
      ;;
      p)
        preserve_data=1
      ;;
      ?)
        shift $(($OPTIND-2))
        usage "Command rmfs: Unrecognized option: $1"
      ;;
    esac
  done

  # sanity checks
  is_root               || die "Only root can remove a repository"
  ensure_enabled_apache_modules

  # get repository names
  shift $(($OPTIND-1))
  check_parameter_count_for_multiple_repositories $#
  names=$(get_or_guess_multiple_repository_names "$@")
  check_multiple_repository_existence "$names"

  for name in $names; do

    # better ask the user again!
    if [ $force -ne 1 ]; then
      local reply
      local question=""
      if [ $preserve_data -eq 0 ]; then
        question="You are about to WIPE OUT THE CERNVM-FS REPOSITORY ${name} INCLUDING SIGNING KEYS!"
      else
        question="You are about to REMOVE THE CERNVM-FS REPOSITORY INFRASTRUCTURE for ${name}!"
      fi

      read -p "${question}  Are you sure (y/N)? " reply
      if [ "$reply" != "y" ] && [ "$reply" != "Y" ]; then
        continue
      fi
    fi

    # get information about repository
    load_repo_config $name

    # check if repository is compatible to the installed CernVM-FS version
    check_repository_compatibility $name

    # sanity checks
    [ x"$CVMFS_SPOOL_DIR"        = x ] && { echo "Spool directory for $name is undefined";  retcode=1; continue; }
    [ x"$CVMFS_UPSTREAM_STORAGE" = x ] && { echo "Upstream storage for $name is undefined"; retcode=1; continue; }
    [ x"$CVMFS_REPOSITORY_TYPE"  = x ] && { echo "Repository type for $name is undefined";  retcode=1; continue; }

    # do it!
    if [ "$CVMFS_REPOSITORY_TYPE" = "stratum0" ]; then
      check_autofs_on_cvmfs && die "Autofs on /cvmfs has to be disabled"
      echo -n "Unmounting CernVM-FS Area... "
      unmount_and_teardown_repository $name || die "fail"
      echo "done"
    fi

    echo -n "Removing Spool Area... "
    remove_spool_area $name
    echo done

    echo -n "Removing Configuration... "
    remove_config_files $name || die "fail"
    echo "done"

    if [ $preserve_data -eq 0 ] && \
       is_local_upstream $CVMFS_UPSTREAM_STORAGE; then
      echo -n "Removing Repository Storage... "
      local storage_dir="$(get_upstream_config $CVMFS_UPSTREAM_STORAGE)"
      if [ x"$storage_dir" != x"" ]; then
        rm -fR "$storage_dir" || die "fail"
      fi
      echo "done"
    fi

    if [ $preserve_data -eq 0 ] && \
       [ "$CVMFS_REPOSITORY_TYPE" = stratum0 ]; then
      echo -n "Removing Keys and Certificate... "
      rm -f /etc/cvmfs/keys/$name.masterkey \
            /etc/cvmfs/keys/$name.pub       \
            /etc/cvmfs/keys/$name.key       \
            /etc/cvmfs/keys/$name.crt || die "fail"
      rm -f /etc/cvmfs/keys/$name.gw
      echo "done"
    fi

    echo -n "Updating global JSON information... "
    update_global_repository_info && echo "done" || echo "fail"

    echo "CernVM-FS repository $name wiped out!"

  done

  return $retcode
}


