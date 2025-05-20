#
# This file is part of the CernVM File System
# This script takes care of creating, removing, and maintaining repositories
# on a Stratum 0/1 server
#
# Implementation of the "cvmfs_server snapshot" command

# This file depends on functions implemented in the following files:
# - cvmfs_server_util.sh
# - cvmfs_server_common.sh


__snapshot_cleanup() {
  local alias_name=$1

  load_repo_config $alias_name
  local user_shell="$(get_user_shell $alias_name)"
  $user_shell "$(__swissknife_cmd) remove     \
                 -r ${CVMFS_UPSTREAM_STORAGE} \
                 -o .cvmfs_is_snapshotting"       || echo "Warning: failed to remove .cvmfs_is_snapshotting"

  release_update_lock $alias_name
}

__snapshot_succeeded() {
  local alias_name=$1
  __snapshot_cleanup $alias_name
  to_syslog_for_repo $alias_name "successfully snapshotted from $CVMFS_STRATUM0"
}

__snapshot_failed() {
  local alias_name=$1
  __snapshot_cleanup $alias_name
  to_syslog_for_repo $alias_name "failed to snapshot from $CVMFS_STRATUM0"
}

__do_snapshot() {
  local alias_names="$1"
  local abort_on_conflict=$2
  local alias_name
  local name
  local user
  local spool_dir
  local stratum0
  local upstream
  local num_workers
  local public_key
  local timeout
  local retries
  local retcode=0
  local gc_timespan=0

  for alias_name in $alias_names; do

    # sanity checks
    is_stratum1 $alias_name || { echo "Repository $alias_name is not a stratum 1 repository"; retcode=1; continue; }

    # get repository information
    CVMFS_PASSTHROUGH=false
    load_repo_config $alias_name
    name=$CVMFS_REPOSITORY_NAME
    user=$CVMFS_USER
    spool_dir=$CVMFS_SPOOL_DIR
    stratum0=$CVMFS_STRATUM0
    stratum1=$CVMFS_STRATUM1
    upstream=$CVMFS_UPSTREAM_STORAGE
    num_workers=$CVMFS_NUM_WORKERS
    public_key=$CVMFS_PUBLIC_KEY
    timeout=$CVMFS_HTTP_TIMEOUT
    retries=$CVMFS_HTTP_RETRIES

    # more sanity checks
    is_owner_or_root $alias_name || { echo "Permission denied: Repository $alias_name is owned by $user"; retcode=1; continue; }
    check_repository_compatibility $alias_name
    [ ! -z $stratum1 ] || die "Missing CVMFS_STRATUM1 URL in server.conf"
    gc_timespan="$(get_auto_garbage_collection_timespan $alias_name)" || { retcode=1; continue; }
    if is_local_upstream $upstream && is_root && check_apache; then
      # this might have been missed if add-replica -a was used or
      #  if a migrate was done while apache wasn't running, but then
      #  apache was enabled later
      # unfortunately we can only check it if snapshot is run as root...
      check_wsgi_module
    fi

    # do it!

    if is_local_upstream $upstream && [ x"$CVMFS_GEO_AUTO_UPDATE" = x"true" ]; then
        # try to update the geodb, but continue if it doesn't work
        _update_geodb -l || true
    fi

    if [ x"$CVMFS_PASSTHROUGH" = x"true" ]; then
      echo "Pass-through repository, skipping snapshot"
      continue
    fi

    if ! acquire_update_lock $alias_name snapshot $abort_on_conflict; then
      retcode=1
      continue
    fi

    local user_shell="$(get_user_shell $alias_name)"

    # here the lock is acquired and needs to be cleared in case of abort
    trap "__snapshot_failed $alias_name" EXIT HUP INT TERM
    to_syslog_for_repo $alias_name "started snapshotting from $stratum0"

    local initial_snapshot=0
    local initial_snapshot_flag=""
    if $user_shell "$(__swissknife_cmd) peek -d .cvmfs_last_snapshot -r ${upstream}" | grep -v -q "available"; then
      initial_snapshot=1
      initial_snapshot_flag="-i"
    fi

    local log_level=
    [ "x$CVMFS_LOG_LEVEL" != x ] && log_level="-l $CVMFS_LOG_LEVEL"
    if [ $initial_snapshot -eq 1 ]; then
      echo "Initial snapshot"
    fi

    # put a magic file in the repository root to signal a snapshot in progress
    local snapshotting_tmp="${spool_dir}/tmp/snapshotting"
    $user_shell "date --utc > $snapshotting_tmp"
    $user_shell "$(__swissknife_cmd) upload -r ${upstream} \
      -i $snapshotting_tmp                                 \
      -o .cvmfs_is_snapshotting"
    $user_shell "rm -f $snapshotting_tmp"

    # do the actual snapshot actions
    local with_history=""
    local with_reflog=""
    local timestamp_threshold=""
    [ $initial_snapshot -ne 1 ] && with_history="-p"
    [ $initial_snapshot -eq 1 ] && \
      with_reflog="-R $(get_reflog_checksum $alias_name)"
    has_reflog_checksum $alias_name && \
      with_reflog="-R $(get_reflog_checksum $alias_name)"
    is_stratum0_garbage_collectable $alias_name &&
      timestamp_threshold="-Z $gc_timespan"
    $user_shell "$(__swissknife_cmd dbg) pull -m $name \
        -u $stratum0                                   \
        -w $stratum1                                   \
        -r ${upstream}                                 \
        -x ${spool_dir}/tmp                            \
        -k $public_key                                 \
        -n $num_workers                                \
        -t $timeout                                    \
        -a $retries $with_history $with_reflog         \
           $initial_snapshot_flag $timestamp_threshold $log_level"

    update_repo_status $alias_name last_snapshot "`date --utc`"

    # this part is deprecated but keep for now for backward compatibility
    local last_snapshot_tmp="${spool_dir}/tmp/last_snapshot"
    $user_shell "date --utc > $last_snapshot_tmp"
    $user_shell "$(__swissknife_cmd) upload -r ${upstream} \
      -i $last_snapshot_tmp                                \
      -o .cvmfs_last_snapshot"
    $user_shell "rm -f $last_snapshot_tmp"
    syncfs cautious

    # run the automatic garbage collection (if configured)
    if is_due_auto_garbage_collection $alias_name; then
      echo "Running automatic garbage collection"
      local dry_run=0
      __run_gc "$alias_name" \
               "$stratum1"   \
               "$dry_run"    \
               ""            \
               "0"           \
               -z $gc_timespan || die "Garbage collection failed ($?)"
    fi

    # all done, clear the trap and run the cleanup manually
    trap - EXIT HUP INT TERM
    __snapshot_succeeded $alias_name

  done

  return $retcode
}

__do_all_snapshots() {
  local separate_logs=0
  local logrotate_nowarn=0
  local skip_noninitial=0
  local snapshot_group
  local log
  local fullog
  local repo
  local repos

  OPTIND=1
  while getopts "snig:" option; do
    case $option in
      s)
        separate_logs=1
      ;;
      n)
        logrotate_nowarn=1
      ;;
      i)
        skip_noninitial=1
      ;;
      g)
        snapshot_group=$OPTARG
      ;;
      ?)
        shift $(($OPTIND-2))
        usage "Command snapshot -a: Unrecognized option: $1"
      ;;
    esac
  done
  shift $(($OPTIND-1))

  if [ ! -d /var/log/cvmfs ]; then
    if ! mkdir /var/log/cvmfs 2>/dev/null; then
      die "/var/log/cvmfs does not exist and could not create it"
    fi
  fi
  [ -w /var/log/cvmfs ] || die "cannot write to /var/log/cvmfs"

  local maxparallel="${CVMFS_MAX_PARALLEL_SNAPSHOTS:-`nproc`}"
  local fulllog=/var/log/cvmfs/snapshots.log

  # make locks in a tmpfs directory so they will go away after system crash
  local tmpdir=/dev/shm/cvmfs_snapshot_all
  if [ ! -d $tmpdir ]; then
    # This assumes that snapshot -a will only be run by a single user id
    # on a given machine.
    mkdir $tmpdir
  fi
  local locknum=0
  local lockfile=""
  while [ "$locknum" -lt "$maxparallel" ]; do
    lockfile=$tmpdir/$locknum
    if acquire_lock $lockfile; then
      break
    fi
    let locknum+=1
  done
  if [ "$locknum" -ge "$maxparallel" ]; then
    # Note that these messages will be the only things in $fullog if 
    # separate logs are being used.
    (echo; echo "Hit limit of $maxparallel parallel 'snapshot -a's at `date`, exiting") >>$fulllog
    exit
  fi

  if [ $separate_logs -eq 0 ]; then
    # write into a temporary file in case more than one is active at the
    #  same time
    log=$tmpdir/$locknum.log
    trap "release_lock $lockfile; rm -f $log" EXIT HUP INT TERM
    (echo; echo "Logging in $log at `date`") >>$fulllog
  else
    trap "release_lock $lockfile" EXIT HUP INT TERM
  fi

  # Sort the active repositories by last snapshot time when on local storage.
  # For non-local, swissknife only supports checking whether a file exists,
  #  so only check whether non-initial snapshots are being skipped.
  repos="$(for replica in /etc/cvmfs/repositories.d/*/replica.conf; do

    # get repository information
    local repodir="${replica%/*}"
    repo="${repodir##*/}"

    if [ "$repo" = "*" ]; then
      # no replica.conf files were found
      continue
    fi

    if is_inactive_replica $repo; then
      continue
    fi

    # unset this first, for backward compatibility with versions that
    #  did not set it
    unset CVMFS_SNAPSHOT_GROUP

    load_repo_config $repo

    if [ "x$CVMFS_SNAPSHOT_GROUP" != "x$snapshot_group" ]; then
      continue
    fi

    local upstream=$CVMFS_UPSTREAM_STORAGE
    local snapshot_time=0
    if is_local_upstream $upstream; then
      local storage_dir=$(get_upstream_config $upstream)
      local snapshot_file=$storage_dir/.cvmfs_last_snapshot
      if cvmfs_sys_file_is_regular $snapshot_file ; then
        snapshot_time="$(stat --format='%Y' $snapshot_file)"
      elif [ $skip_noninitial -eq 1 ]; then
        continue
      fi
    elif [ $skip_noninitial -eq 1 ]; then
      if $user_shell "$(__swissknife_cmd) peek -d .cvmfs_last_snapshot -r ${upstream}" | grep -v -q "available"; then
        continue
      fi
    fi

    echo "${snapshot_time}:${repo}"

  done|sort -n|cut -d: -f2)"

  for repo in $repos; do
    if [ $separate_logs -eq 1 ]; then
      log=/var/log/cvmfs/$repo.log
    fi

    (
    echo
    echo "Starting $repo at `date`"
    # Work around the errexit (that is, set -e) misfeature of being
    #  disabled whenever the exit code is to be checked.
    # See https://lists.gnu.org/archive/html/bug-bash/2012-12/msg00093.html
    set +e
    (set -e
    __do_snapshot $repo 1
    )
    if [ $? != 0 ]; then
      echo "ERROR from cvmfs_server snapshot!" >&2
    fi
    echo "Finished $repo at `date`"
    ) >> $log 2>&1

    if [ $separate_logs -eq 0 ]; then
      cat $log >>$fulllog
      > $log
    fi

  done
}

cvmfs_server_snapshot() {
  local alias_names
  local retcode=0
  local abort_on_conflict=0
  local do_all=0
  local allopts=""

  OPTIND=1
  while getopts "atsnig:" option; do
    case $option in
      a)
        do_all=1
      ;;
      s|n|i)
        allopts="$allopts -$option"
      ;;
      g)
        allopts="$allopts -g $OPTARG"
      ;;
      t)
        abort_on_conflict=1
      ;;
      ?)
        shift $(($OPTIND-2))
        usage "Command snapshot: Unrecognized option: $1"
      ;;
    esac
  done
  shift $(($OPTIND-1))

  if [ $do_all -eq 1 ]; then
    [ $# -eq 0 ] || die "no non-option parameters expected with -a"

    # ignore if there's a -t option, it's always implied with -a

    __do_all_snapshots $allopts

    # always return success because this is used from cron and we
    #  don't want cron sending an email every time something fails
    # errors will be in the log

  else
    if [ -n "$allopts" ]; then
      usage "Command snapshot:$allopts unrecognized without -a"
    fi

    # get repository names
    check_parameter_count_for_multiple_repositories $#
    alias_names=$(get_or_guess_multiple_repository_names "$@")
    check_multiple_repository_existence "$alias_names"

    __do_snapshot "$alias_names" $abort_on_conflict
    retcode=$?
  fi

  return $retcode
}

