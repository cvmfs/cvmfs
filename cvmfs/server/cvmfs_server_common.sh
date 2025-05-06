#
# This file is part of the CernVM File System
# This script takes care of creating, removing, and maintaining repositories
# on a Stratum 0/1 server
#
# Implementation of functionality common to all "cvmfs_server" commands
#


# This file depends on functions implemented in the following files:
# - cvmfs_server_util.sh
# - cvmfs_server_json.sh


# checks the parameter count for a situation where we might be able to guess
# the repository name based on the repositories present in the system
# Note: if the parameter count does not fit or if guessing is impossible,
#       this will print the usage string with an error message and exit
# Note: this method is commonly used right before invoking
#       `get_or_guess_repository_name` to check its preconditions and report
#       error before actually doing something wrong
#
# @param provided_parameter_count  number of parameters provided by the user
# @param allow_multiple_names      switches off the usage print for too many
#                                  detected script parameters (see next fn)
check_parameter_count_with_guessing() {
  local provided_parameter_count=$1
  local allow_multiple_names=$2

  if [ $provided_parameter_count -lt 1 ]; then
    # check if we have not _exactly_ one repository present
    if [ $(ls /etc/cvmfs/repositories.d/ | wc -l) -ne 1 ]; then
      usage "Please provide a repository name"
    fi
  fi

  if [ $provided_parameter_count -gt 1 ] && \
     [ x"$allow_multiple_names" = x"" ]; then
    usage "Too many arguments provided"
  fi

  return 0
}


# checks the parameter count when we accept more than one repository for the
# command.
# Note: this method prints an error message if appropriate and exists the script
#       execution
#
# @param provided_parameter_count  number of parameters provided by the user
check_parameter_count_for_multiple_repositories() {
  local provided_parameter_count=$1
  check_parameter_count_with_guessing $provided_parameter_count allow_multiple
  return $?
}


# checks if the right number of arguments was provided
# if the wrong number was provided it will kill the script after printing the
# usage text and an error message
#
# @param expected_parameter_count   number of expected parameters
# @param provided_parameter_count   number of provided parameters
check_parameter_count() {
  local expected_parameter_count=$1
  local provided_parameter_count=$2

  if [ $provided_parameter_count -lt $expected_parameter_count ]; then
    usage "Too few arguments provided"
  fi
  if [ $provided_parameter_count -gt $expected_parameter_count ]; then
    usage "Too many arguments provided"
  fi
}


# guesses a list of repository names based on file system wildcards
#
# @param ...    all repository hints provided by the user of the script
#               Like: test.local repo.* *.cern.ch
get_or_guess_multiple_repository_names() {
  local repo_dir="/etc/cvmfs/repositories.d"
  local repo_names=""

  if [ $# -eq 0 ]; then
    repo_names=$(get_or_guess_repository_name)
    echo $repo_names
    return 0;
  fi

  for input_pattern in "$@"; do
    local names="$(ls --directory $repo_dir/$input_pattern 2>/dev/null)"
    if [ x"$names" = x"" ]; then
      repo_names="$repo_names $input_pattern"
    else
      for name in $names; do
        if ! contains "$repo_names" $(basename $name); then
          repo_names="$repo_names $(basename $name)"
        fi
      done
    fi
  done

  echo "$repo_names"
}


# checks if the given repository name already exists
#
# @param given_name   the name of the repository to be checked
# @return             0 if the repository was found
check_repository_existence() {
  local given_name="$1"
  local fqrn

  # empty name or wildcards are not allowed (and thus does not exist)
  if [ x"$given_name" = x ] || echo "$given_name" | grep -q "*"; then
    return 1
  fi

  # Check if exists
  fqrn=$(cvmfs_mkfqrn $given_name)
  [ -d /etc/cvmfs/repositories.d/$fqrn ]
}


# checks the existence of a list of repositories
# Note: the function echo's an error message and stops the execution of the
#       script by default.
#
# @param given_names   the list of repository names to be checked
# @param no_kill       (optional) skip the termination on error
# @return              0 if all listed repositories exist
check_multiple_repository_existence() {
  local given_names="$1"
  local no_kill=$2

  for name in $given_names; do
    # If "name" contains a subpath (i.e. repo.cern.ch/sub/path) only
    # the repository name should be kept
    local repo_name=$(echo $name | cut -d'/' -f1)
    if ! check_repository_existence $repo_name; then
      if [ x"$no_kill" = x"" ]; then
        die "The repository $repo_name does not exist."
      else
        return 1
      fi
    fi
  done
  return 0
}


# mangles the repository name into a fully qualified repository name
#
# @param repository_name       the repository name given by the user
# @return                      echoes the correct repository name to use
get_repository_name() {
  local repository_name=$1
  echo $(cvmfs_mkfqrn $repository_name)
}


# loads the configuration for a specific repository
load_repo_config() {
  local name=$1
  if [ ! -e /etc/cvmfs/repositories.d/${name}/server.conf ]; then
    die "Error: The repository $name does not exist."
  fi
  . /etc/cvmfs/repositories.d/${name}/server.conf
  if [ x"$CVMFS_REPOSITORY_TYPE" = x"stratum0" ]; then
    . /etc/cvmfs/repositories.d/${name}/client.conf
  else
    . /etc/cvmfs/repositories.d/${name}/replica.conf
  fi
}


# retrieves (or guesses) the version of CernVM-FS that was used to create this
# repository.
# @param name  the name of the repository to be checked
repository_creator_version() {
  local name="$1"
  load_repo_config $name
  local version="$CVMFS_CREATOR_VERSION"
  if [ x"$version" = x ]; then
    version="2.1.6" # 2.1.6 was the last version, that did not store the creator
                    # version... therefore this has to be handled as "<= 2.1.6"
                    # Note: see also `mangle_version_string()`
  elif [ x"$version" = x"2.2.0" ]; then
    version="2.2.0-0" # CernVM-FS 2.2.0-0 was a server-only pre-release which is
                      # incompatible with 2.2.0-1
                      # 2.2.0-0 marks itself as CVMFS_CREATOR_VERSION=2.2.0
                      # while 2.2.0-1 features  CVMFS_CREATOR_VERSION=2.2.0-1
  fi
  echo $version
}


# gets the catalog root hash associated to a tag name
#
# @param repository_name   the name of the repository to be checked
# @param tag               the tag name to be checked
# @return                  hash value or the empty string
get_tag_hash() {
  local repository_name="$1"
  local tag="$2"

  load_repo_config $repository_name
  __swissknife tag_info                       \
    -w $CVMFS_STRATUM0                        \
    -t ${CVMFS_SPOOL_DIR}/tmp                 \
    -p /etc/cvmfs/keys/${repository_name}.pub \
    -f $repository_name                       \
    $(get_swissknife_proxy)                   \
    $(get_follow_http_redirects_flag) -x      \
    -n "$tag" 2>/dev/null | cut -d" " -f2
}


# gets the branch associated to a tag name
#
# @param repository_name   the name of the repository to be checked
# @param tag               the tag name to be checked
# @return                  branch name
get_tag_branch() {
  local repository_name="$1"
  local tag="$2"

  load_repo_config $repository_name
  local branch=$(__swissknife tag_info        \
    -w $CVMFS_STRATUM0                        \
    -t ${CVMFS_SPOOL_DIR}/tmp                 \
    -p /etc/cvmfs/keys/${repository_name}.pub \
    -f $repository_name                       \
    $(get_swissknife_proxy)                   \
    $(get_follow_http_redirects_flag) -x      \
    -n "$tag" 2>/dev/null | cut -d" " -f6)
  if [ "x$branch" = "x(default)" ]; then
    branch=
  fi
  echo "$branch"
}


# checks if a given tag already exists in the repository's history database
#
# @param repository_name   the name of the repository to be checked
# @param tag               the tag name to be checked
# @return                  0 if tag already exists
check_tag_existence() {
  local repository_name="$1"
  local tag="$2"

  local tag_hash=$(get_tag_hash $repository_name $tag)
  [ "x$tag_hash" != "x" ]
}


# gets the youngest tag on a given branch
#
# @param repository_name   the name of the repository to be checked
# @param branch            the branch name to be checked
# @return                  '<tag name> <hash> <branch>'
get_head_of() {
  local repository_name="$1"
  local branch="$2"

  load_repo_config $repository_name
  __swissknife tag_list                       \
    -w $CVMFS_STRATUM0                        \
    -t ${CVMFS_SPOOL_DIR}/tmp                 \
    -p /etc/cvmfs/keys/${repository_name}.pub \
    -f $repository_name                       \
    $(get_swissknife_proxy)                   \
    $(get_follow_http_redirects_flag) -x      \
    | cut -d" " -f1,2,6 | grep " $branch\$" | head -n 1
}


# retrieves the currently mounted root catalog hash in the spool area
#
# @param repository_name    the name of the repository to be checked
# @return                   echoes the currently mounted root catalog hash
get_mounted_root_hash() {
  local repository_name=$1

  load_repo_config $repository_name
  get_repo_info -u ${CVMFS_SPOOL_DIR}/rdonly -C
}


# retrieves the latest published root catalog hash in the backend storage
#
# @param repository_name   the name of the repository to be checked
# @return                  echoes the last published (HEAD) root catalog hash
get_published_root_hash() {
  local repository_name=$1

  load_repo_config $repository_name
  get_repo_info -c
}

# retrieves the current manifest without signature
#
# @param repository_name   the name of the repository to be checked
# @return                  echoes the published manufest
get_raw_manifest() {
  local repository_name=$1

  load_repo_config $repository_name
  get_repo_info -R
}


set_ro_root_hash() {
  local name=$1
  local root_hash=$2
  local client_config=/var/spool/cvmfs/${name}/client.local

  load_repo_config $name

  local upstream_type=$(get_upstream_type $CVMFS_UPSTREAM_STORAGE)

  if [ x"$upstream_type" = xgw ]; then
      sed -i -e "s/CVMFS_ROOT_HASH=.*//" $client_config
  else
      if grep -q ^CVMFS_ROOT_HASH= ${client_config}; then
          sed -i -e "s/CVMFS_ROOT_HASH=.*/CVMFS_ROOT_HASH=${root_hash}/" $client_config
      else
          echo "CVMFS_ROOT_HASH=${root_hash}" >> $client_config
      fi
  fi
}


get_repo_info_from_url() {
  local url="$1"
  shift 1
  __swissknife info $(get_follow_http_redirects_flag) $(get_swissknife_proxy) -r "$url" $@ 2>/dev/null
}


# expects load_repo_config to be called with the right repository before
# possible parameters, see `cvmfs_swissknife info`
get_repo_info() {
  get_repo_info_from_url "$CVMFS_STRATUM0" $@
}


# checks if a repository is currently in a transaction
#
# @param name  the repository name to be checked
# @return      0 if in a transaction
is_in_transaction() {
  local name=$1
  load_repo_config $name
  local tx_flag="${CVMFS_SPOOL_DIR}/in_transaction"
  check_flag "${tx_flag}"
}


# checks if a repository is currently running a publish procedure
#
# @param name  the repository name to be checked
# @return      0 if a publishing procedure is running
is_publishing() {
  local name=$1
  load_repo_config $name
  check_lock ${CVMFS_SPOOL_DIR}/is_publishing
}

# checks if a repository is currently checked out
#
# @param name  the repository name to be checked
# @return      0 if checked out
is_checked_out() {
  local name=$1
  load_repo_config $name
  [ -f /var/spool/cvmfs/${name}/checkout ]
}


# parses the checkout file
#
# @param name  the repository name to be checked
# @return      0 if checked out
get_checked_out_tag() {
  local name=$1
  load_repo_config $name
  cat /var/spool/cvmfs/${name}/checkout | cut -d" " -f1
}


# parses the checkout file
#
# @param name  the repository name to be checked
# @return      0 if checked out
get_checked_out_hash() {
  local name=$1
  load_repo_config $name
  cat /var/spool/cvmfs/${name}/checkout | cut -d" " -f2
}


# parses the checkout file
#
# @param name  the repository name to be checked
# @return      0 if checked out
get_checked_out_branch() {
  local name=$1
  load_repo_config $name
  cat /var/spool/cvmfs/${name}/checkout | cut -d" " -f3
}


# parses the checkout file
#
# @param name  the repository name to be checked
# @return      0 if checked out
get_checked_out_previous_branch() {
  local name=$1
  load_repo_config $name
  cat /var/spool/cvmfs/${name}/checkout | cut -d" " -f4
}


# checks if the running user is root
#
# @return   0 if the current user is root
is_root() {
  [ $(id -u) -eq 0 ]
}


# checks if the running user is either the owner of a repository or root
#
# @param name  the name of the repository
is_owner_or_root() {
  local name="$1"
  is_root && return 0
  load_repo_config $name
  [ x"$(whoami)" = x"$CVMFS_USER" ]
}


# checks if a repository is a replica flagged as being inactive
#
# @param name  the name of the repository to be checked
# @return      0 if it is an inactive replica
is_inactive_replica() {
  local name=$1
  unset CVMFS_REPLICA_ACTIVE # remove previous setting, default is yes
  load_repo_config $name
  is_stratum1 $name && [ x"$CVMFS_REPLICA_ACTIVE" = x"no" ]
}


# checks if a repository is flagged as being garbage collectable
# (this is a safe guard to avoid mistakenly deleting data in production repos)
#
# @param name  the name of the repository to be checked
# @return      0 if it is garbage collectable
is_garbage_collectable() {
  local name=$1
  load_repo_config $name
  if is_stratum0 $name; then
    [ x"$CVMFS_GARBAGE_COLLECTION" = x"true" ]
  else
    [ x"$(get_repo_info_from_url $CVMFS_STRATUM1 -g)" = x"yes" ]
  fi
}


# checks if a repository has automatic garbage collection enabled and was
# not garbage collected for long enough to justify an automatic run
#
# @param name  the name of the repository to be checked
# @return      0 if automatic garbage collection should run
is_due_auto_garbage_collection() {
  local name=$1
  load_repo_config $name

  is_garbage_collectable $name || return 1
  [ x"$CVMFS_AUTO_GC" = x"true" ] || return 1

  CVMFS_AUTO_GC_LAPSE="${CVMFS_AUTO_GC_LAPSE:-$CVMFS_DEFAULT_AUTO_GC_LAPSE}"
  local gc_deadline=$(date --date "$CVMFS_AUTO_GC_LAPSE" +%s 2>/dev/null)
  if [ -z "$gc_deadline" ]; then
    echo "Failed to parse CVMFS_AUTO_GC_LAPSE: '$CVMFS_AUTO_GC_LAPSE'" >&2
    return 0
  fi

  local gc_status="$(read_repo_item $name .cvmfs_status.json)"
  local last_gc="$(get_json_field "$gc_status" last_gc)"
  [ ! -z "$last_gc" ] || return 0
  last_gc=$(date --date "$last_gc" +%s 2>/dev/null)
  if [ -z "$last_gc" ]; then
    echo "Failed to parse last gc timestamp: '$last_gc'" >&2
    return 0
  fi

  [ $last_gc -lt $gc_deadline ]
}


# download a given file from the backend storage
# @param name  the name of the repository to download from
# @param url   the url to download from
get_item() {
  local name="$1"
  local url="$2"

  load_repo_config $name

  curl -f -H "Cache-Control: max-age=0" $(get_curl_proxy) \
       $(get_x509_cert_settings) $(get_follow_http_redirects_flag) \
       "$url" 2>/dev/null | tr -d '\0'
}

# read an item from local or backend repository storage to stdout
# @param name  the name of the repository to download from
# @param item  the name of the item to download
# if the file does not exist, there will be no output and the
#  return will be an error code
read_repo_item() {
  local name="$1"
  local item="$2"

  load_repo_config $name

  if is_local_upstream $CVMFS_UPSTREAM_STORAGE; then
    cat $(get_upstream_config $CVMFS_UPSTREAM_STORAGE)/"$item" 2>/dev/null
  elif is_stratum0 $name; then
    get_item $name $CVMFS_STRATUM0/"$item"
  else
    get_item $name $CVMFS_STRATUM1/"$item"
  fi
}

# Parse redirects configuration into redirect flag.
# Assumes that config is loaded already, i.e. load_repo_config().
get_follow_http_redirects_flag() {
  if [ x"$CVMFS_FOLLOW_REDIRECTS" = x"yes" ]; then
    echo "-L"
  fi
}


# Parse special CA path settings for curl invocation
get_x509_cert_settings() {
  if [ x"$X509_CERT_BUNDLE" != "x" ]; then
      echo "--cacert $X509_CERT_BUNDLE"
  fi
}

# Parse proxy server for curl command
get_curl_proxy() {
  if [ x"$CVMFS_SERVER_PROXY" != x"" ]; then
    echo "-x $CVMFS_SERVER_PROXY"
  fi
}

# Parse proxy server for cvmfs_swissknife command
get_swissknife_proxy() {
  if [ x"$CVMFS_SERVER_PROXY" != x"" ]; then
    echo "-@ $CVMFS_SERVER_PROXY"
  fi
}

get_expiry_from_string() {
  local whitelist="$1"

  local expires=$(echo "$whitelist" | grep --text -e '^E[0-9]\{14\}$' | tr -d E)
  if echo "$expires" | grep -q -E --invert-match '^[0-9]{14}$'; then
    echo -1
    return 1
  fi
  local year=$(echo $expires | head -c4)
  local month=$(echo $expires | head -c6 | tail -c2)
  local day=$(echo $expires | head -c8 | tail -c2)
  local hour=$(echo $expires | head -c10 | tail -c2)
  local minute=$(echo $expires | head -c12 | tail -c2)
  local second=$(echo $expires | head -c14 | tail -c2)
  local expires_fmt="${year}-${month}-${day} ${hour}:${minute}:${second}"
  local expires_num=$(date -u -d "$expires_fmt" +%s)

  local now=$(/bin/date -u +%s)
  local valid_countdown=$(( $expires_num-$now ))
  echo $valid_countdown
}


# figures out the time to expiry of the repository's whitelist
#
# @param stratum0  path/URL to stratum0 storage
# @return          number of seconds until expiry (negative if already expired)
get_expiry() {
  local name=$1
  local stratum0=$2
  get_expiry_from_string "$(get_item $name $stratum0/.cvmfswhitelist)"
}


# checks if the repository's whitelist is valid
#
# @param stratum0  path/URL to stratum0 storage
# @return          0 if whitelist is still valid
check_expiry() {
  local name=$1
  local stratum0=$2
  local expiry="-1"

  expiry=$(get_expiry $name $stratum0)
  if [ $? -ne 0 ]; then
    echo "Failed to retrieve repository expiry date" >&2
    return 100
  fi

  [ $expiry -ge 0 ]
}


# create a shell invocation to be used by commands to impersonate the owner of
# a specific CVMFS repository.
# Note: when impersonating the non-root repository owner, root's environment is
#       kept (usually including root's `umask` - see integration test 571)
#
# @param name   the name of the repository whose owner should be impersonated
# @return       a shell invocation to impersonate $CVMFS_USER via stdout or
#               exit code 1 if user couldn't be impersonated
get_user_shell() {
  local name=$1
  local shell_cmd=""

  load_repo_config $name
  if [ x"$(whoami)" = x"$CVMFS_USER" ]; then
    shell_cmd="sh -c"
  elif is_root; then
    if [ $HAS_RUNUSER -ne 0 ]; then
      shell_cmd="$RUNUSER_BIN -m $CVMFS_USER -c"
    else
      shell_cmd="su -m $CVMFS_USER -c"
    fi
  fi

  echo "$shell_cmd"
  [ ! -z "$shell_cmd" ] # return false if no suitable shell could be constructed
}


sign_manifest() {
  local name=$1
  local unsigned_manifest=$2
  local metainfo_file=$3
  local return_early=
  if [ "x$4" = "xtrue" ]; then
    return_early="-e"
  fi

  load_repo_config $name
  local user_shell="$(get_user_shell $name)"

  local sign_command="$(__swissknife_cmd) sign \
          -c /etc/cvmfs/keys/${name}.crt       \
          -k /etc/cvmfs/keys/${name}.key       \
          -n $name                             \
          -u $CVMFS_STRATUM0                   \
          -m $unsigned_manifest                \
          -t ${CVMFS_SPOOL_DIR}/tmp            \
          $(get_swissknife_proxy)              \
          -r $CVMFS_UPSTREAM_STORAGE $return_early"

  if [ x"$metainfo_file" != x"" ]; then
    sign_command="$sign_command -M $metainfo_file"
  fi
  if [ x"$CVMFS_GARBAGE_COLLECTION" = x"true" ]; then
    sign_command="$sign_command -g"
  fi
  if [ x"$CVMFS_CATALOG_ALT_PATHS" = x"true" ]; then
    sign_command="$sign_command -A"
  fi
  if has_reflog_checksum $name; then
    sign_command="$sign_command -R $(get_reflog_checksum $name)"
  fi

  $user_shell "$sign_command" > /dev/null
}


open_transaction() {
  local name=$1
  load_repo_config $name
  local tx_flag="${CVMFS_SPOOL_DIR}/in_transaction"

  is_stratum0 $name                    || die "Cannot open transaction on Stratum1"
  set_flag "$tx_flag"                  || die "Failed to create transaction flag"
  run_suid_helper open $name           || die "Failed to make /cvmfs/$name writable"

  to_syslog_for_repo $name "opened transaction"
}


# closes a previously opened transaction
# Note: This function will perform remounts on /cvmfs/${name} and the underlying
#       read-only CVMFS branch. Hence, check for open file descriptors first!
#
# @param name             the repository whose transaction should be closed
# @param use_fd_fallback  if set != 0 this will perform a violent remount of the
#                         repository to handle potential open file descriptors
#                         on /cvmfs/${name}
close_transaction() {
  local name=$1
  local use_fd_fallback=$2

  is_in_transaction $name || return 0

  load_repo_config $name
  local tx_flag="${CVMFS_SPOOL_DIR}/in_transaction"
  local tmp_dir="${CVMFS_SPOOL_DIR}/tmp"
  local current_scratch_dir="${CVMFS_SPOOL_DIR}/scratch/current"
  local wastebin_scratch_dir="${CVMFS_SPOOL_DIR}/scratch/wastebin"
  local force_grace_time=60

  # if not explicitly asked, try if umounting works without force
  if [ $use_fd_fallback -eq 0 ]; then
    if ! run_suid_helper rw_umount     $name || \
       ! run_suid_helper rdonly_umount $name; then
      use_fd_fallback=1
    fi
  fi

  # if explicitly asked for or the normal umount failed we apply more force
  if [ $use_fd_fallback -ne 0 ]; then
    if [ x"$CVMFS_FORCE_REMOUNT_WARNING" != x"false" ]; then
      (
        echo "$name is forcefully remounted in $force_grace_time seconds."
        echo "Please close files on /cvmfs/$name"
      ) | wall 2>/dev/null
      sleep $force_grace_time
      echo "$name is forcefully remounted NOW." | wall 2>/dev/null
    fi
    run_suid_helper rw_lazy_umount     $name
    run_suid_helper kill_cvmfs         $name
    run_suid_helper rdonly_lazy_umount $name
  fi

  # continue with the remounting
  local async_msg=""
  if [ x"$CVMFS_ASYNC_SCRATCH_CLEANUP" != x"false" ]; then
    tmpdir=$(mktemp -d "${wastebin_scratch_dir}/waste.XXXXXX")
    if mv $current_scratch_dir $tmpdir; then
      mkdir -p $current_scratch_dir && chown $CVMFS_USER $current_scratch_dir
      async_msg="(asynchronous scratch cleanup)"
      run_suid_helper clear_scratch_async $name
    else
      to_syslog_for_repo $name \
        "asynchronous cleanup failed, doing synchronous cleanup"
      run_suid_helper clear_scratch $name
    fi
  else
    run_suid_helper clear_scratch $name
  fi
  # Prevent "argument too long" errors
  [ ! -z "$tmp_dir" ] && find "${tmp_dir}" -mindepth 1 -not -path '*/receiver*' | xargs rm -fR
  run_suid_helper rdonly_mount $name > /dev/null
  run_suid_helper rw_mount $name
  clear_flag "$tx_flag"

  # Remove session_token file, used for gateway transactions, if it exists
  rm -f ${CVMFS_SPOOL_DIR}/session_token

  local fallback_msg=""
  [ $use_fd_fallback -eq 0 ] || fallback_msg="(using force)"
  to_syslog_for_repo $name "closed transaction $fallback_msg $async_msg"
}


# Release an update lock
#
# @param name             the repository to release

release_update_lock() {
  local name=$1

  load_repo_config $name
  release_lock ${CVMFS_SPOOL_DIR}/is_updating || echo "Warning: failed to release updating lock"
}

# Acquire an update lock for a repository.  Always pair with a call to
#   release_update_lock if returns successful.
#
# @param name               the repository to lock
# @param update_type        update type such as snapshot or gc
# @param abort_on_conflict  0 to wait for lock, 1 to abort if already acquired.
#                           Default 0.  Always aborts if initial snapshot is
#                           in progress.
# @return                   0 if lock successfully acquired

acquire_update_lock()
{
  local name=$1
  local update_type=$2
  local abort_on_conflict=${3:-0}

  load_repo_config $name
  local update_lock=${CVMFS_SPOOL_DIR}/is_updating

  # check for other updates in progress
  if ! acquire_lock $update_lock; then
    if [ $abort_on_conflict -eq 1 ]; then
      echo "another update is in progress... aborting"
      to_syslog_for_repo $name "did not $update_type (another update in progress)"
      return 1
    fi

    local user_shell="$(get_user_shell $name)"
    local initial_snapshot=0
    if $user_shell "$(__swissknife_cmd) peek -d .cvmfs_last_snapshot -r $CVMFS_UPSTREAM_STORAGE" | grep -v -q "available"; then
      initial_snapshot=1
    fi

    if [ $initial_snapshot -eq 1 ]; then
      echo "an initial snapshot is in progress... aborting"
      to_syslog_for_repo $name "did not $update_type (initial snapshot in progress)"
      return 1
    fi

    echo "waiting for another update to finish..."
    if ! wait_and_acquire_lock $update_lock; then
      echo "failed to acquire update lock"
      to_syslog_for_repo $name "did not $update_type (locking issues)"
      return 1
    fi
  fi

  # The lock is now acquired
}


# Release a gc lock
#
# @param name             the repository to release

release_gc_lock() {
  local name=$1

  load_repo_config $name
  release_lock ${CVMFS_SPOOL_DIR}/is_collecting || echo "Warning: failed to release gc lock"
}

# Acquire a gc lock for a repository.  Always pair with a call to
#   release_gc_lock if returns successful.
#
# @param name               the repository to lock
# @param check_type         check type, either check or gc
# @param abort_on_conflict  0 to wait for lock, 1 to abort if already acquired.
#                           Default 0.
# @return                   0 if lock successfully acquired

acquire_gc_lock()
{
  local name=$1
  local check_type=$2
  local abort_on_conflict=${3:-0}

  load_repo_config $name
  local gc_lock=${CVMFS_SPOOL_DIR}/is_collecting

  # look for another check or gc in progress
  if ! acquire_lock $gc_lock; then
    if [ $abort_on_conflict -eq 1 ]; then
      echo "A check or other gc on $name is in progress... aborting"
      to_syslog_for_repo $name "did not $check_type (check or gc in progress)"
      return 1
    fi

    echo "Waiting for gc on $name to finish..."
    if ! wait_and_acquire_lock $gc_lock; then
      echo "failed to acquire gc lock"
      to_syslog_for_repo $name "did not $check_type (locking issues)"
      return 1
    fi
  fi

  # The lock is now acquired
}


handle_read_only_file_descriptors_on_mount_point() {
  local name=$1
  local open_fd_dialog=${2:-1}

  if [ $(count_rd_only_fds /cvmfs/$name) -eq 0 ]; then
    return 0
  fi

  if [ $open_fd_dialog -eq 1 ]; then
    file_descriptor_warning_and_question $name # might abort...
  else
    file_descriptor_warning $name
  fi

  return 1
}


file_descriptor_warning_and_question() {
  local name=$1
  echo "\

WARNING! There are open read-only file descriptors in /cvmfs/$name
  --> This is potentially harmful and might cause problems later on.
      We can anyway perform the requested operation, but this will most likely
      break other processes with open file descriptors on /cvmfs/$name!

      The following lsof report might show the processes with open file handles
      "

  generate_lsof_report_for_mountpoint "/cvmfs/${name}"

  echo -n "\

         Do you want to proceed anyway? (y/N) "

  local reply="n"
  read reply
  if [ "$reply" != "y" ] && [ "$reply" != "Y" ]; then
    echo "aborted."
    exit 1
  fi

  return 0
}


file_descriptor_warning() {
  local name=$1

  echo "WARNING: Open file descriptors on /cvmfs/$name (possible race!)"
  echo "         The following lsof report might show the culprit:"
  echo
  generate_lsof_report_for_mountpoint "/cvmfs/${name}"
  echo
}


generate_lsof_report_for_mountpoint() {
  local mountpoint="$1"
  $LSOF_BIN | awk '{print $1,$2,$3,$NF}' | column -t | grep "$mountpoint" || true
}


# puts all configuration files in place that are need for a stratum0 repository
#
# @param name        the name of the repository
# @param upstream    the upstream definition of the future repository
# @param stratum0    the URL of the stratum0 http entry point
# @param cvmfs_user  the owning user of the repository
create_config_files_for_new_repository() {
  local name=$1
  local upstream=$2
  local stratum0=$3
  local cvmfs_user=$4
  local unionfs=$5
  local hash_algo=$6
  local autotagging=$7
  local garbage_collectable=$8
  local configure_apache=$9
  local compression_alg=${10}
  local external_data=${11}
  local voms_authz=${12}
  local auto_tag_timespan="${13}"
  local proxy_url=${14}

  # other configurations
  local spool_dir="/var/spool/cvmfs/${name}"
  local scratch_dir="${spool_dir}/scratch/current"
  local rdonly_dir="${spool_dir}/rdonly"
  local temp_dir="${spool_dir}/tmp"
  local cache_dir="${spool_dir}/cache"
  local repo_cfg_dir="/etc/cvmfs/repositories.d/${name}"
  local server_conf="${repo_cfg_dir}/server.conf"
  local client_conf="${repo_cfg_dir}/client.conf"

  mkdir -p $repo_cfg_dir
  cat > $server_conf << EOF
# Created by cvmfs_server.
CVMFS_CREATOR_VERSION=$(cvmfs_layout_revision)
CVMFS_REPOSITORY_NAME=$name
CVMFS_REPOSITORY_TYPE=stratum0
CVMFS_USER=$cvmfs_user
CVMFS_UNION_DIR=/cvmfs/$name
CVMFS_SPOOL_DIR=$spool_dir
CVMFS_STRATUM0=$stratum0
CVMFS_UPSTREAM_STORAGE=$upstream
CVMFS_USE_FILE_CHUNKING=$CVMFS_DEFAULT_USE_FILE_CHUNKING
CVMFS_MIN_CHUNK_SIZE=$CVMFS_DEFAULT_MIN_CHUNK_SIZE
CVMFS_AVG_CHUNK_SIZE=$CVMFS_DEFAULT_AVG_CHUNK_SIZE
CVMFS_MAX_CHUNK_SIZE=$CVMFS_DEFAULT_MAX_CHUNK_SIZE
CVMFS_UNION_FS_TYPE=$unionfs
CVMFS_HASH_ALGORITHM=$hash_algo
CVMFS_COMPRESSION_ALGORITHM=$compression_alg
CVMFS_EXTERNAL_DATA=$external_data
CVMFS_AUTO_TAG=$autotagging
CVMFS_AUTO_TAG_TIMESPAN="$auto_tag_timespan"
CVMFS_GARBAGE_COLLECTION=$garbage_collectable
CVMFS_AUTO_REPAIR_MOUNTPOINT=true
CVMFS_AUTOCATALOGS=false
CVMFS_ASYNC_SCRATCH_CLEANUP=true
CVMFS_PRINT_STATISTICS=false
CVMFS_UPLOAD_STATS_DB=false
CVMFS_UPLOAD_STATS_PLOTS=false
CVMFS_IGNORE_XDIR_HARDLINKS=true
EOF

  if [ x"$voms_authz" != x"" ]; then
    echo "CVMFS_VOMS_AUTHZ=$voms_authz" >> $server_conf
    echo "CVMFS_CATALOG_ALT_PATHS=true" >> $server_conf
  fi

  # append GC specific configuration
  if [ x"$garbage_collectable" = x"true" ]; then
    cat >> $server_conf << EOF
CVMFS_AUTO_GC=true
EOF
  fi

  if [ x"$proxy_url" != x"" ]; then
    echo "CVMFS_SERVER_PROXY=$proxy_url" >> $server_conf
  fi

  if [ $configure_apache -eq 1 ] && is_local_upstream $upstream; then
    local repository_dir=$(get_upstream_config $upstream)
    # make sure that the config file does not exist, yet
    remove_apache_config_file "$(get_apache_conf_filename $name)" || true
    create_apache_config_for_endpoint $name $repository_dir
    create_apache_config_for_global_info
  fi

  cat > $client_conf << EOF
# Created by cvmfs_server.  Don't touch.
CVMFS_CACHE_BASE=$cache_dir
CVMFS_RELOAD_SOCKETS=$cache_dir
CVMFS_QUOTA_LIMIT=4000
CVMFS_MOUNT_DIR=/cvmfs
CVMFS_SERVER_URL=$stratum0
CVMFS_HTTP_PROXY=${proxy_url:-DIRECT}
CVMFS_PUBLIC_KEY=/etc/cvmfs/keys/${name}.pub
CVMFS_CHECK_PERMISSIONS=yes
CVMFS_IGNORE_SIGNATURE=no
CVMFS_AUTO_UPDATE=no
CVMFS_NFS_SOURCE=no
CVMFS_MAGIC_XATTRS_VISIBILITY=never
CVMFS_FOLLOW_REDIRECTS=yes
CVMFS_SERVER_CACHE_MODE=yes
CVMFS_NFILES=65536
CVMFS_TALK_SOCKET=/var/spool/cvmfs/${name}/cvmfs_io
CVMFS_TALK_OWNER=$cvmfs_user
CVMFS_USE_SSL_SYSTEM_CA=true
EOF

  if [ "x$X509_CERT_BUNDLE" != "x" ]; then
    cat >> $client_conf << EOF
X509_CERT_BUNDLE=$X509_CERT_BUNDLE
EOF
  fi
}


create_spool_area_for_new_repository() {
  local name=$1

  # gather repository information from configuration file
  load_repo_config $name
  local spool_dir=$CVMFS_SPOOL_DIR
  local current_scratch_dir="${spool_dir}/scratch/current"
  local wastebin_scratch_dir="${spool_dir}/scratch/wastebin"
  local rdonly_dir="${spool_dir}/rdonly"
  local temp_dir="${spool_dir}/tmp"
  local cache_dir="${spool_dir}/cache"
  local ofs_workdir="${spool_dir}/ofs_workdir"

  mkdir -p /cvmfs/$name          \
           $current_scratch_dir  \
           $wastebin_scratch_dir \
           $rdonly_dir           \
           $temp_dir             \
           $cache_dir || return 1
  if [ x"$CVMFS_UNION_FS_TYPE" = x"overlayfs" ]; then
    mkdir -p $ofs_workdir || return 2
  fi
  chown -R $CVMFS_USER /cvmfs/$name/ $spool_dir/
}


remove_spool_area() {
  local name=$1
  load_repo_config $name
  [ x"$CVMFS_SPOOL_DIR" != x"" ] || return 0
  rm -fR "$CVMFS_SPOOL_DIR"      || return 1
  if [ -d /cvmfs/$name ]; then
    rmdir /cvmfs/$name           || return 2
  fi
}


create_global_info_skeleton() {
  local info_path="$(get_global_info_path)"
  local info_v1_path="$(get_global_info_v1_path)"

  mkdir -p $info_path                               || return 1
  mkdir -p $info_v1_path                            || return 2
  set_selinux_httpd_context_if_needed $info_path    || return 3
  set_selinux_httpd_context_if_needed $info_v1_path || return 4

  _check_info_file "repositories" || echo "{}" | _write_info_file "repositories"
  _check_info_file "meta" || _write_info_file "meta" << EOF
{
  "administrator" : "Your Name",
  "email"         : "you@organisation.org",
  "organisation"  : "Your Organisation",

  "custom" : {
    "_comment" : "Put arbitrary structured data here"
  }
}
EOF
}


create_repository_skeleton() {
  local directory=$1
  local user=$2

  echo -n "Creating repository skeleton in ${directory}..."
  mkdir -p ${directory}/data
  local i=0
  while [ $i -lt 256 ]
  do
    mkdir -p ${directory}/data/$(printf "%02x" $i)
    i=$(($i+1))
  done
  mkdir -p ${directory}/data/txn
  if [ x$(id -un) != x$user ]; then
    chown -R $user ${directory}/
  fi
  set_selinux_httpd_context_if_needed $directory
  echo "done"
}


create_repository_storage() {
  local name=$1
  local storage_dir
  load_repo_config $name
  storage_dir=$(get_upstream_config $CVMFS_UPSTREAM_STORAGE)
  create_repository_skeleton $storage_dir $CVMFS_USER > /dev/null
}


create_repometa_skeleton() {
  local json_file="$1"
  cat > "$json_file" << EOF
{
  "administrator" : "Your Name",
  "email"         : "you@organisation.org",
  "organisation"  : "Your Organisation",
  "description"   : "Repository content",
  "url"           : "Project website",
  "recommended-stratum0":  "stratum 0 url",
  "recommended-stratum1s" : [ "stratum1 url", "stratum1 url" ],

  "custom" : {
    "_comment" : "Put arbitrary structured data here"
  }
}
EOF
}


setup_and_mount_new_repository() {
  local name=$1
  local http_timeout=15

  # get repository information
  load_repo_config $name
  local rdonly_dir="${CVMFS_SPOOL_DIR}/rdonly"
  local scratch_dir="${CVMFS_SPOOL_DIR}/scratch/current"
  local ofs_workdir="${CVMFS_SPOOL_DIR}/ofs_workdir"

  local selinux_context=""
  if [ x"$CVMFS_UNION_FS_TYPE" = x"overlayfs" ]; then
    echo -n "(overlayfs) "
    cat >> /etc/fstab << EOF
cvmfs2#$name $rdonly_dir fuse allow_other,fsname=$name,config=/etc/cvmfs/repositories.d/${name}/client.conf:${CVMFS_SPOOL_DIR}/client.local,cvmfs_suid,noauto 0 0 # added by CernVM-FS for $name
overlay_$name /cvmfs/$name overlay upperdir=${scratch_dir},lowerdir=${rdonly_dir},workdir=$ofs_workdir,noauto,nodev,ro 0 0 # added by CernVM-FS for $name
EOF
  else
    echo -n "(aufs) "
    if has_selinux && try_mount_remount_cycle_aufs; then
      selinux_context=",context=\"system_u:object_r:default_t:s0\""
    fi
    cat >> /etc/fstab << EOF
cvmfs2#$name $rdonly_dir fuse allow_other,fsname=$name,config=/etc/cvmfs/repositories.d/${name}/client.conf:${CVMFS_SPOOL_DIR}/client.local,cvmfs_suid,noauto 0 0 # added by CernVM-FS for $name
aufs_$name /cvmfs/$name aufs br=${scratch_dir}=rw:${rdonly_dir}=rr,udba=none,noauto,nodev,ro$selinux_context 0 0 # added by CernVM-FS for $name
EOF
  fi
  local user_shell="$(get_user_shell $name)"
  $user_shell "touch ${CVMFS_SPOOL_DIR}/client.local"

  # avoid racing against apache; we can safely ignore the certificate validation
  # at this step, we only want to check that the endpoint is up.
  # NB: Normally, we are anyway dealing with HTTP URLs at this point.
  local waiting=0
  while ! curl $(get_curl_proxy) --insecure -sIf ${CVMFS_STRATUM0}/.cvmfspublished > /dev/null && \
        [ $http_timeout -gt 0 ]; do
    [ $waiting -eq 1 ] || echo -n "waiting for apache... "
    waiting=1
    http_timeout=$(( $http_timeout - 1 ))
    sleep 1
  done
  [ $http_timeout -gt 0 ] || return 1

  mount $rdonly_dir > /dev/null || return 1
  mount /cvmfs/$name

  # Make sure the systemd mount unit exists
  if is_systemd; then
    /usr/lib/systemd/system-generators/systemd-fstab-generator \
      /run/systemd/generator '' '' 2>/dev/null || true
    systemctl daemon-reload
  fi
}


# checks if the (corresponding) stratum 0 is garbage collectable
#
# @param name  the name of the stratum1/stratum0 repository to be checked
# @return      0 if it is garbage collectable
is_stratum0_garbage_collectable() {
  local name=$1
  load_repo_config $name
  [ x"$(get_repo_info_from_url $CVMFS_STRATUM0 -g)" = x"yes" ]
}


# checks if a manifest is present
#
# @param url  the url of the repository to be checked
# @return      0 if it is empty
is_empty_repository_from_url() {
  local url=$1
  [ x"$(get_repo_info_from_url "$url" -e)" = x"yes" ]
}


# checks if a manifest is present
#
# @param name  the name of the repository to be checked
# @return      0 if it is empty
is_empty_repository() {
  local name=$1
  local url=""
  load_repo_config $name
  is_stratum0 $name && url="$CVMFS_STRATUM0" || url="$CVMFS_STRATUM1"
  [ x"$(get_repo_info_from_url "$url" -e)" = x"yes" ]
}

# checks if a repository contains a reference log that is necessary to run
# garbage collections
#
# @param name  the name of the repository to be checked
# @return      0 if it contains a reference log
has_reference_log() {
  local name=$1
  local url=""
  load_repo_config $name
  is_stratum0 $name && url="$CVMFS_STRATUM0" || url="$CVMFS_STRATUM1"
  [ x"$(get_repo_info_from_url "$url" -o)" = x"true" ]
}


# get the configured (or default) timespan for an automatic garbage
# collection run.
#
# @param name  the name of the repository to be checked
# @return      the configured CVMFS_AUTO_GC_TIMESPAN or default (3 days ago)
#              as a timestamp threshold (unix timestamp)
#              Note: in case of a malformed timespan it might print an error to
#                     stderr and return a non-zero code
get_auto_garbage_collection_timespan() {
  local name=$1
  local timespan="3 days ago"

  load_repo_config $name
  if [ ! -z "$CVMFS_AUTO_GC_TIMESPAN" ]; then
    timespan="$CVMFS_AUTO_GC_TIMESPAN"
  fi

  if ! date --date "$timespan" +%s 2>/dev/null; then
    echo "Failed to parse CVMFS_AUTO_GC_TIMESPAN: '$timespan'" >&2
    return 1
  fi
}


# mangles the repository name into a fully qualified repository name
# if there was no repository name given and there is only one repository present
# in the system, it automatically returns the name of this one.
#
# @param repository_name  the name of the repository to work on (might be empty)
# @return                 echoes a suitable repository name
get_or_guess_repository_name() {
  local repository_name=$1

  if [ "x$repository_name" = "x" ]; then
    echo $(get_repository_name $(ls /etc/cvmfs/repositories.d))
  else
    echo $(get_repository_name $repository_name)
  fi
}


# get the configured timespan for removing old auto-generated tags.
#
# @param name  the name of the repository to be checked
# @return      the configured CVMFS_AUTO_TAG_TIMESPAN or 0 (forever)
#              as a timestamp threshold (unix timestamp)
#              Note: in case of a malformed timespan it might print an error to
#                     stderr and return a non-zero code
get_auto_tags_timespan() {
  local repository_name=$1

  load_repo_config $repository_name
  local timespan="$CVMFS_AUTO_TAG_TIMESPAN"
  if [ -z "$timespan" ]; then
    echo "0"
    return 0
  fi

  if ! date --date "$timespan" +%s 2>/dev/null; then
    echo "Failed to parse CVMFS_AUTO_TAG_TIMESPAN: '$timespan'" >&2
    return 1
  fi
  return 0
}


unmount_and_teardown_repository() {
  local name=$1
  load_repo_config $name
  sed -i -e "/added by CernVM-FS for ${name}/d" /etc/fstab
  local rw_mnt="/cvmfs/$name"
  local rdonly_mnt="${CVMFS_SPOOL_DIR}/rdonly"
  is_mounted "$rw_mnt"     && ( umount $rw_mnt     || return 1; )
  is_mounted "$rdonly_mnt" && ( umount $rdonly_mnt || return 2; )
  return 0
}


_run_catalog_migration() {
  local name="$1"
  local migration_command="$2"

  load_repo_config $name

  # more sanity checks
  is_stratum0 $name       || die "This is not a stratum 0 repository"
  is_root                 || die "Permission denied: Only root can do that"
  is_in_transaction $name && die "Repository is already in a transaction"
  health_check -r $name

  # all following commands need an open repository transaction and are supposed
  # to commit or abort it after performing the catalog migration.
  echo "Opening repository transaction"
  trap "close_transaction $name 0" EXIT HUP INT TERM
  open_transaction $name || die "Failed to open repository transaction"

  # run the catalog migration operation (must run as root!)
  echo "Starting catalog migration"
  local tmp_dir=${CVMFS_SPOOL_DIR}/tmp
  local manifest=${tmp_dir}/manifest
  migration_command="${migration_command} -t $tmp_dir -o $manifest"
  sh -c "$migration_command" || die "Fail (executed command: $migration_command)"

  # check if the catalog migration created a new revision
  if [ ! -f $manifest ]; then
    echo "Catalog migration finished without any changes"
    return 0
  fi

  # finalizing transaction
  local trunk_hash=$(grep "^C" $manifest | tr -d C)
  echo "Flushing file system buffers"
  syncfs

  # committing newly created revision
  echo "Signing new manifest"
  chown $CVMFS_USER $manifest        || die "chmod of new manifest failed";
  sign_manifest $name $manifest      || die "Signing failed";
  set_ro_root_hash $name $trunk_hash || die "Root hash update failed";
}
