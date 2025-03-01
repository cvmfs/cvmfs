#
# This file is part of the CernVM File System
# This script takes care of creating, removing, and maintaining repositories
# on a Stratum 0/1 server
#
# Implementation of the "cvmfs_server resign" command

# This file depends on functions implemented in the following files:
# - cvmfs_server_util.sh
# - cvmfs_server_common.sh

cvmfs_server_resign() {
  local names
  local retcode=0
  local expire_days
  local force=0
  local whitelist_path
  local sign_published=0

  # parameter handling
  OPTIND=1
  while getopts "d:fpw:" option; do
    case $option in
      d)
        expire_days=$OPTARG
      ;;
      f)
        force=1
      ;;
      p)
        sign_published=1
      ;;
      w)
        whitelist_path=$OPTARG
      ;;
      ?)
        shift $(($OPTIND-2))
        usage "Command resign: Unrecognized option: $1"
      ;;
    esac
  done

  # get repository names
  shift $(($OPTIND-1))
  check_parameter_count_for_multiple_repositories $#
  names=$(get_or_guess_multiple_repository_names "$@")
  [ -n "$whitelist_path" ] || check_multiple_repository_existence "$names"

  # sanity checks
  [ $sign_published -eq 0 ] || [ -z "$expire_days" ]    || die "Cannot use -d with -p"
  [ $sign_published -eq 0 ] || [ -z "$whitelist_path" ] || die "Cannot use both -w and -p"
  [ $sign_published -eq 1 ] || is_root || die "Only root can resign whitelists"

  if [ $sign_published -eq 0 ] && \
        [ -n "$expire_days" ] && [ $expire_days -gt 30 ]; then
    echo "Warning: whitelist expiration is more than 30 days."
    echo "Long expirations increase risk from repository key compromises!"
    if [ $force -ne 1 ]; then
      local reply
      read -p "Are you sure you want to do this (y/N)? " reply
      if [ "$reply" != "y" ] && [ "$reply" != "Y" ]; then
        return 1
      fi
    fi
  fi

  for name in $names; do

    if [ -z "$whitelist_path" ]; then
      # sanity checks
      is_stratum0 $name  || { echo "Repository $name is not a stratum 0 repository"; retcode=1; continue; }

      # get repository information
      load_repo_config $name

      # check if repository is compatible to the installed CernVM-FS version
      check_repository_compatibility $name

      # do it!
      if [ $sign_published -eq 1 ]; then
        # This is intended to be used when a repository key has been changed
        # It reuses everything from an old .cvmfspublished except the
        #  certificate hash, signature, and timestamp.

        echo -n "Signing .cvmfspublished... "
        local manifest="${CVMFS_SPOOL_DIR}/tmp/manifest"
        local manifest_url="${CVMFS_STRATUM0}/.cvmfspublished"
        local user_shell="$(get_user_shell $name)"
        # create the temporary manifest file with user permission first
        #  which will work whether running as the user or root
        $user_shell "> $manifest"
        local old_manifest
        old_manifest="`get_item $name $manifest_url`" || die "fail (manifest download)!"
        # overwriting will not change the owner
        echo "$old_manifest" | strip_manifest_signature - > $manifest
        sign_manifest $name $manifest
        echo "done"

      else

        create_whitelist $name $CVMFS_USER \
            ${CVMFS_UPSTREAM_STORAGE} ${CVMFS_SPOOL_DIR}/tmp "$expire_days"

      fi
    else
      # do not require repository configuration, just the whitelist file
      [ -f $whitelist_path ] || { echo "$whitelist_path does not exist!"; retcode=1; continue; }

      local user tmpdir
      user="`stat --format=%U $whitelist_path`"
      tmpdir="`mktemp -d`"
      trap "rm -rf $tmpdir" EXIT HUP INT TERM

      create_whitelist $name $user "" $tmpdir "$expire_days" $whitelist_path

      rm -rf $tmpdir
      trap - EXIT HUP INT TERM
    fi

  done

  return $retcode
}


