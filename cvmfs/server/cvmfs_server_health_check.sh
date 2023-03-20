#
# This file is part of the CernVM File System
# This script takes care of creating, removing, and maintaining repositories
# on a Stratum 0/1 server
#

# This file depends on functions implemented in the following files:
# - cvmfs_server_util.sh
# - cvmfs_server_common.sh


# Checks for inconsistent repository states or unfavorable configs. Can repair
# inconsistent repository mount states (-r)
# Parameters:
#   -q    silence notifications to stdout/stderr (syslog messages stay in place)
#   -r    try to repair detected inconsistencies (non-zero exit on failure)
#   -t    repair even if the repository is in a transaction
#   -f    force repair (even if CVMFS_AUTO_REPAIR_MOUNTPOINT=false)
#
# @param name   the FQRN of the repository to be checked
# @return       0 if repository is healthy (or has been successfully repaired)
#               otherwise 1 (or abort when -r is given and repair fails)
health_check() {
  local name=""
  local gateway=0
  local quiet=0
  local repair=0
  local repair_in_txn=0
  local force_repair=0

  OPTIND=0
  while getopts "gqrtf" option; do
    case $option in
      g)
        gateway=1
      ;;
      q)
        quiet=1
      ;;
      r)
        repair=1
      ;;
      t)
        repair=1
        repair_in_txn=1
      ;;
      f)
        repair=1
        force_repair=1
      ;;
      ?)
        shift $(($OPTIND-2))
        die "health_check: Unrecognized option: $1"
      ;;
    esac
  done
  shift $(($OPTIND-1))

  [ $# -eq 1 ] || die "health_check: No repository name provided"
  name=$(get_repository_name $1)

  local rdonly_broken=0
  local rw_broken=0
  local rw_should_be_rdonly=0
  local rw_should_be_rw=0
  local rdonly_outdated=0
  local rdonly_wronghash=0

  load_repo_config $name

  # for stratum 1 repositories there are no health checks
  if is_stratum1 $name; then
    return 0
  fi

  # check mounted read-only cvmfs client
  local expected_hash=
  if ! is_mounted "${CVMFS_SPOOL_DIR}/rdonly"; then
    rdonly_broken=1
  elif [ x"$(get_mounted_root_hash $name)"      != \
         x"$(get_published_root_hash $name)" ]; then
    if ! is_checked_out $name; then
      expected_hash=$(get_published_root_hash $name)
      rdonly_outdated=1
    else
      expected_hash=$(get_checked_out_hash $name)
      if [ x"$(get_mounted_root_hash $name)" != x"$expected_hash" ]; then
        rdonly_wronghash=1
      fi
    fi
  fi

  # check mounted union file system
  if ! is_mounted "/cvmfs/$name"; then
    rw_broken=1
    if is_in_transaction $name; then
      rw_should_be_rw=1
      rw_should_be_ro=0
    else
      rw_should_be_rw=0
      rw_should_be_ro=1
    fi
  else
    if ! is_in_transaction $name && \
         is_mounted "/cvmfs/$name" "^.* rw[, ].*$"; then
      rw_should_be_rdonly=1
    elif is_in_transaction $name && \
         is_mounted "/cvmfs/$name" "^.* ro[, ].*$"; then
      rw_should_be_rw=1
    fi
  fi

  # did we detect any kind of problem?
  local ok=$(( $rdonly_broken       \
             + $rw_broken           \
             + $rw_should_be_rdonly \
             + $rw_should_be_rw ))
  if [ $gateway -eq 0 ]; then
    ok=$(( $ok + $rdonly_wronghash + $rdonly_outdated ))
  fi
  if [ $ok -eq 0 ]; then
    return 0
  fi

  # should we print the found status?
  if [ $quiet = 0 ]; then
    __hc_print_status_report $name $rdonly_broken       \
                                   $rdonly_outdated     \
                                   $rdonly_wronghash    \
                                   $rw_broken           \
                                   $rw_should_be_rdonly \
                                   $rw_should_be_rw
  fi

  # should we try a repair?
  if [ $repair = 0 ]; then
    return 1
  fi

  # check if we are allowed to attempt a repair
  if [ x"$CVMFS_AUTO_REPAIR_MOUNTPOINT" = x"false" ] && \
     [ $force_repair = 0 ]; then
    echo "Auto-Repair is disabled (CVMFS_AUTO_REPAIR_MOUNTPOINT = false)" >&2
    exit 1
  fi

  if is_publishing $name; then
    echo "WARNING: The repository $name is currently publishing and should not" >&2
    echo "be touched. If you are absolutely sure, that this is _not_ the case," >&2
    echo "please run the following command and retry:"                          >&2
    echo                                                                        >&2
    echo "   rm -fR ${CVMFS_SPOOL_DIR}/is_publishing.lock"                      >&2
    echo                                                                        >&2
    exit 1
  fi

  if is_in_transaction $name && [ $repair_in_txn = 0 ]; then
    echo "Repository $name is in a transaction and cannot be repaired." >&2
    echo "--> Run \`cvmfs_server abort $name\` to revert and repair."   >&2
    exit 1
  fi

  to_syslog_for_repo $name "attempting mountpoint repair ($rdonly_broken $rdonly_outdated $rw_broken $rw_should_be_rdonly $rw_should_be_rw)"

  # consecutively bring the mountpoints into a sane state by working bottom up:
  #   1. solve problems with the rdonly mountpoint
  #      Note: this might require to 'break' the rw mountpoint (rw_broken --> 1)
  #      1.1. solve outdated rdonly mountpoint (rdonly_outdated --> 0)
  #      1.2. remount rdonly mountpoint        (rdonly_broken   --> 0)
  #   2. solve problems with the rw mountpoint
  #      2.1. mount the rw mountpoint as read-only    (rw_broken       --> 0)
  #      2.2. remount the rw mountpoint as read-only  (rw_should_be_ro --> 0)
  #      2.2. remount the rw mountpoint as read-write (rw_should_be_rw --> 0)
  if [ $(($rdonly_outdated + $rdonly_wronghash)) -gt 0 ]; then
    if [ $rw_broken -eq 0 ]; then
      __hc_transition $name $quiet "rw_umount"
      rw_broken=1 # ... remount happens downstream
    fi

    if [ $rdonly_broken -eq 0 ]; then
      __hc_transition $name $quiet "rdonly_umount"
      rdonly_broken=1 # ... remount happens downstream
    fi

    set_ro_root_hash $name "$expected_hash" || die "failed to update root hash"
    rdonly_outdated=0 # ... remount will mount the latest revision
    rdonly_wronghash=0
  fi

  if [ $rdonly_broken -eq 1 ]; then
    if [ $rw_broken -eq 0 ]; then
      __hc_transition $name $quiet "rw_umount"
      rw_broken=1 # ... remount happens downstream
    fi

    __hc_transition $name $quiet "rdonly_mount"
    rdonly_broken=0 # ... rdonly is repaired
  fi

  if [ $rw_broken -eq 1 ]; then
    __hc_transition $name $quiet "rw_mount"
    rw_broken=0           # ... rw is repaired
    rw_should_be_rdonly=0 # ... and already mounted read-only by default
  fi

  if [ $rw_should_be_rw -eq 1 ]; then
    __hc_transition $name $quiet "open"
    rw_should_be_rw=0 # ... rw is repaired
  fi

  if [ $rw_should_be_rdonly -eq 1 ]; then
    __hc_transition $name $quiet "lock"
    rw_should_be_rdonly=0 # ... rw is repaired
  fi

  to_syslog_for_repo $name "finished mountpoint repair ($rdonly_broken $rdonly_outdated $rw_broken $rw_should_be_rdonly $rw_should_be_rw)"
}


