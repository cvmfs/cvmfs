#
# This file is part of the CernVM File System
# This script takes care of creating, removing, and maintaining repositories
# on a Stratum 0/1 server
#
# Implementation of functionality common to all "cvmfs_server" commands
#


# This file depends on fuctions implemented in the following files:
# - cvmfs_server_util.sh


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

  for input_pattern in $@; do
    local names="$(ls --directory $repo_dir/$input_pattern 2>/dev/null)"
    if [ x"$names" = x"" ]; then
      repo_names="$repo_names $input_pattern"
    else
      for name in $names; do
        if ! contains "$repo_names" $(basename $name); then
          repo_names="$(basename $name) $repo_names"
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
    if ! check_repository_existence $name; then
      if [ x"$no_kill" = x"" ]; then
        die "The repository $name does not exist."
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


# checks if a given tag already exists in the repository's history database
#
# @param repository_name   the name of the repository to be checked
# @param tag               the tag name to be checked
# @return                  0 if tag already exists
check_tag_existence() {
  local repository_name="$1"
  local tag="$2"

  load_repo_config $repository_name
  __swissknife tag_info                       \
    -w $CVMFS_STRATUM0                        \
    -t ${CVMFS_SPOOL_DIR}/tmp                 \
    -p /etc/cvmfs/keys/${repository_name}.pub \
    -f $repository_name                       \
    -n "$tag" > /dev/null 2>&1
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


set_ro_root_hash() {
  local name=$1
  local root_hash=$2
  local client_config=/var/spool/cvmfs/${name}/client.local

  if grep -q ^CVMFS_ROOT_HASH= ${client_config}; then
    sed -i -e "s/CVMFS_ROOT_HASH=.*/CVMFS_ROOT_HASH=${root_hash}/" $client_config
  else
    echo "CVMFS_ROOT_HASH=${root_hash}" >> $client_config
  fi
}


get_repo_info_from_url() {
  local url="$1"
  shift 1
  __swissknife info $(get_follow_http_redirects_flag) -r "$url" $@ 2>/dev/null
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
  check_lock ${CVMFS_SPOOL_DIR}/in_transaction ignore_stale
}


# checks if a repository is currently runing a publish procedure
#
# @param name  the repository name to be checked
# @return      0 if a publishing procedure is running
is_publishing() {
  local name=$1
  load_repo_config $name
  check_lock ${CVMFS_SPOOL_DIR}/is_publishing ignore_stale
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


# checks if a repository has automatic garbage collection enabled
#
# @param name  the name of the repository to be checked
# @return      0 if automatic garbage collection is enabled
has_auto_garbage_collection_enabled() {
  local name=$1
  load_repo_config $name
  is_garbage_collectable $name && [ x"$CVMFS_AUTO_GC" = x"true" ]
}


# download a given file from the backend storage
# @param noproxy  (optional)
get_item() {
  local name="$1"
  local url="$2"
  local noproxy="$3"

  load_repo_config $name

  if [ x"$noproxy" != x"" ]; then
    unset http_proxy && curl $(get_follow_http_redirects_flag) "$url" 2>/dev/null
  else
    curl $(get_follow_http_redirects_flag) "$url" 2>/dev/null
  fi
}


# Parse redirects configuration into redirect flag.
# Assumes that config is loaded already, i.e. load_repo_config().
get_follow_http_redirects_flag() {
  if [ x"$CVMFS_FOLLOW_REDIRECTS" = x"yes" ]; then
    echo "-L"
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
# @return          number of seconds until expiry (negativ if already expired)
get_expiry() {
  local name=$1
  local stratum0=$2
  get_expiry_from_string "$(get_item $name $stratum0/.cvmfswhitelist 'noproxy')"
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

  load_repo_config $name
  local user_shell="$(get_user_shell $name)"

  local sign_command="$(__swissknife_cmd) sign \
          -c /etc/cvmfs/keys/${name}.crt       \
          -k /etc/cvmfs/keys/${name}.key       \
          -n $name                             \
          -u $CVMFS_STRATUM0                   \
          -m $unsigned_manifest                \
          -t ${CVMFS_SPOOL_DIR}/tmp            \
          -r $CVMFS_UPSTREAM_STORAGE"

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
  local tx_lock="${CVMFS_SPOOL_DIR}/in_transaction"

  is_stratum0 $name                    || die "Cannot open transaction on Stratum1"
  acquire_lock "$tx_lock" ignore_stale || die "Failed to create transaction lock"
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
  local tx_lock="${CVMFS_SPOOL_DIR}/in_transaction"
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
    mv $current_scratch_dir $tmpdir
    mkdir -p $current_scratch_dir && chown $CVMFS_USER $current_scratch_dir
    async_msg="(asynchronous scratch cleanup)"
    run_suid_helper clear_scratch_async $name
  else
    run_suid_helper clear_scratch $name
  fi
  [ ! -z "$tmp_dir" ] && rm -fR "${tmp_dir}"/*
  run_suid_helper rdonly_mount $name > /dev/null
  run_suid_helper rw_mount $name
  release_lock "$tx_lock"

  local fallback_msg=""
  [ $use_fd_fallback -eq 0 ] || fallback_msg="(using force)"
  to_syslog_for_repo $name "closed transaction $fallback_msg $async_msg"
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


# prints some help information optionally followed by an error message
# afterwards it aborts the script
#
# @param errormsg   an optional error message that is printed after the
#                   actual usage text
usage() {
  errormsg=$1

  echo "\
CernVM-FS Server Tool $(cvmfs_version_string)

Usage: cvmfs_server COMMAND [options] <parameters>

Supported Commands:
  mkfs            [-w stratum0 url] [-u upstream storage] [-o owner]
                  [-m replicable] [-f union filesystem type] [-s S3 config file]
                  [-g disable auto tags] [-G Set timespan for auto tags]
                  [-a hash algorithm (default: SHA-1)]
                  [-z enable garbage collection] [-v volatile content]
                  [-Z compression algorithm (default: zlib)]
                  [-k path to existing keychain] [-p no apache config]
                  [-V VOMS authorization] [-X (external data)]
                  <fully qualified repository name>
                  Creates a new repository with a given name
  add-replica     [-u stratum1 upstream storage] [-o owner] [-w stratum1 url]
                  [-a silence apache warning] [-z enable garbage collection]
                  [-n alias name] [-s S3 config file] [-p no apache config]
                  <stratum 0 url> <public key>
                  Creates a Stratum 1 replica of a Stratum 0 repository
  import          [-w stratum0 url] [-o owner] [-u upstream storage]
                  [-l import legacy repo (2.0.x)] [-s show migration statistics]
                  [-f union filesystem type] [-c file ownership (UID:GID)]
                  [-k path to keys] [-g chown backend] [-r recreate whitelist]
                  [-p no apache config] [-t recreate repo key and certificate]
                  <fully qualified repository name>
                  Imports an old CernVM-FS repository into a fresh repo
  publish         [-p pause for tweaks] [-n manual revision number] [-v verbose]
                  [-a tag name] [-c tag channel] [-m tag description]
                  [-X (force external data) | -N (force native data)]
                  [-Z compression algorithm] [-F authz info file]
                  [-f use force remount if necessary]
                  <fully qualified name>
                  Make a new repository snapshot
  gc              [-r number of revisions to preserve]
                  [-t time stamp after which revisions are reseved]
                  [-l (print deleted objects)] [-L log of deleted objects]
                  [-f (force)] [-d (dry run)]
                  <fully qualified repository name>
                  Remove unreferenced data from garbage collectable repository
  rmfs            [-p(reserve) repo data and keys] [-f don't ask again]
                  <fully qualified name>
                  Remove the repository
  abort           [-f don't ask again]
                  <fully qualified name>
                  Abort transaction and return to the state before
  rollback        [-t tag] [-f don't ask again]
                  <fully qualified name>
                  Re-publishes the given tag as the new latest revision.
                  All snapshots between trunk and the target tag become
                  inaccessible.  Without a tag name, trunk-previous is used.
  resign          <fully qualified name>
                  Re-sign the 30 day whitelist
  list-catalogs   [-s catalog sizes] [-e catalog entry counts]
                  [-h catalog hashes] [-x machine readable]
                  <fully qualified name>
                  Print a full list of all nested catalogs of a repository
  info            <fully qualified name>
                  Print summary about the repository
  tag             Create and manage named snapshots
                  [-a create tag <name>] [-c channel] [-m message] [-h hash]
                  [-r remove tag <name>] [-f don't ask again]
                  [-i inspect tag <name>] [-x machine readable]
                  [-l list tags] [-x machine readable]
                  <fully qualified name>
                  Print named tags (snapshots) of the repository
  check           [-c disable data chunk existence check]
                  [-i check data integrity] (may take some time)]
                  [-t tag (check given tag instead of trunk)]
                  [-s path to nested catalog subtree to check]
                  <fully qualified name>
                  Checks if the repository is sane
  transaction     <fully qualified name>
                  Start to edit a repository
  snapshot        [-t fail if other snapshot is in progress]
                  <fully qualified name>
                  Synchronize a Stratum 1 replica with the Stratum 0 source
  snapshot -a     [-s use separate logs in /var/log/cvmfs for each repository]
                  [-n do not warn if /etc/logrotate.d/cvmfs does not exist]
                  [-i skip repositories that have not run initial snapshot]
                  Do snapshot on all active replica repositories
  mount           [-a | <fully qualified name>]
                  Mount repositories in /cvmfs, for instance after reboot
  migrate         <fully qualified name>
                  Migrates a repository to the current version of CernVM-FS
  list            List available repositories
  update-geodb    [-l update lazily based on CVMFS_UPDATEGEO* variables]
                  Updates the geo-IP database
  update-info     [-p no apache config] [-e don't edit /info/meta]
                  Open meta info JSON file for editing
  update-repoinfo [-f path to JSON file]
                  <fully qualified name>
                  Open repository meta info JSON file for editing
"


  if [ x"$errormsg" != x ]; then
    echo "\
________________________________________________________________________

NOTE: $errormsg
"
    exit 3
  else
    exit 2
  fi
}

