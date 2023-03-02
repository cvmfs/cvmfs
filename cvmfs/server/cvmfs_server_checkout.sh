#
# This file is part of the CernVM File System
# This script takes care of creating, removing, and maintaining repositories
# on a Stratum 0/1 server
#
# Implementation of the "cvmfs_server checkout" command

# This file depends on functions implemented in the following files:
# - cvmfs_server_util.sh
# - cvmfs_server_common.sh

cvmfs_server_checkout() {
  local name
  local branch_name
  local tag_name
  local tag_hash
  local branch_head

  # optional parameter handling
  OPTIND=1
  while getopts "b:t:" option
  do
    case $option in
      b)
        branch_name="$OPTARG"
      ;;
      t)
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
  check_autofs_on_cvmfs && die "Autofs on /cvmfs has to be disabled"
  check_repository_existence $name || die "The repository $name does not exist"
  load_repo_config $name
  is_owner_or_root $name || die "Permission denied: Repository $name is owned by $CVMFS_USER"
  is_stratum0 $name      || die "This is not a stratum 0 repository"
  ! is_publishing $name  || die "Repository is currently publishing"
  health_check     $name || die "Repository $name is not healthy"

  # check if repository is compatible to the installed CernVM-FS version
  check_repository_compatibility $name

  is_in_transaction $name && die "Cannot checkout while in a transaction"
  trap "close_transaction $name 0" EXIT HUP INT TERM
  open_transaction $name || die "Failed to open transaction for checkout"

  if [ "x$branch_name" != "x" ]; then
    is_valid_branch_name "$branch_name" || die "invalid branch name: $branch_name"
    branch_head=$(get_head_of $name "$branch_name")
  fi

  if [ "x$tag_name" = "x" ]; then
    if [ "x$branch_name" = "x" ]; then
      # Reset to trunk
      set_ro_root_hash $name "$(get_published_root_hash $name)" || die "failed to update root hash"
      rm -f /var/spool/cvmfs/${name}/checkout
      echo "Reset to trunk on default branch"
      return 0
    fi
    # Checkout head of existing branch
    [ "x$branch_head" != "x" ] || die "branch $branch_name does not exist"
    tag_name=$(echo $branch_head | cut -d" " -f1)
    tag_hash=$(echo $branch_head | cut -d" " -f2)
  else
    # checkout into new branch
    [ "x$branch_name" = "x" ] && die "missing branch name"
    [ "x$branch_head" != "x" ] && die "branch $branch_name already exists"
    tag_hash=$(get_tag_hash $name "$tag_name")
    [ "x$tag_hash" != "x" ] || die "tag $tag_name does not exist"
  fi
  local tag_branch=$(get_tag_branch $name "$tag_name")

  set_ro_root_hash $name $tag_hash || die "failed to update root hash"
  echo "$tag_name $tag_hash $branch_name $tag_branch" > /var/spool/cvmfs/${name}/checkout
  if [ x"$(whoami)" = x"$CVMFS_USER" ]; then
    chown $CVMFS_USER /var/spool/cvmfs/${name}/checkout
  fi
  local report_branch="on branch"
  [ "x$branch_head" = "x" ] && report_branch="onto new branch"
  echo "Checked out tag $tag_name ($tag_hash) $report_branch $branch_name"
}
