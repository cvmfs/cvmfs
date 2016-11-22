#
# This file is part of the CernVM File System
# This script takes care of creating, removing, and maintaining repositories
# on a Stratum 0/1 server
#
# Utility functions used by the  "cvmfs_server" script
#


__swissknife_cmd() {
  local might_be_debugging="$1"
  if [ ! -z $might_be_debugging ]; then
    echo "$CVMFS_SERVER_SWISSKNIFE_DEBUG"
  else
    echo "$CVMFS_SERVER_SWISSKNIFE"
  fi
}


__swissknife() {
  $(__swissknife_cmd) $@
}


# checks if a given list of strings contains a specific item
#
# @param haystack   the list to be searched
# @param needle     the string item to be found in the haystack
# @return           0 if the item was found
contains() {
  local haystack="$1"
  local needle=$2

  for elem in $haystack; do
    if [ x"$elem" = x"$needle" ]; then
      return 0
    fi
  done

  return 1
}


# checks if autofs is disabled on /cvmfs
#
# @return  0 if autofs is not used for /cvmfs
check_autofs_on_cvmfs() {
  cat /proc/mounts | grep -q "^/etc/auto.cvmfs /cvmfs "
}


# checks if a given repository is a stratum 0 repository
#
# @param name   the repository name to be checked
# @return       0 if it is a stratum 0 repository
is_stratum0() {
  local name=$1
  ! [ -f /etc/cvmfs/repositories.d/$name/replica.conf ]
}


# checks if a given repository is a stratum 1 repository
#
# @param name   the repository name to be checked
# @return       0 if it is a stratum 1 repository
is_stratum1() {
  local name=$1
  ! is_stratum0 $name
}


# Makes a fully qualified repository name
cvmfs_mkfqrn() {
   local repo=$1

   if [ -z "$repo" ]; then
      echo
      return 0
   fi

   echo $repo | grep \\. || echo "${repo}.${CVMFS_DEFAULT_DOMAIN}"
   return 0
}


# checks if a given path is mounted
# Note: this takes care of symlink resolving and should be
#       used exclusively for mount checks
#
# @param mountpoint  the mountpoint to be checked
# @param regexp      additional regexp to be validated on the mountpoint record
#                    found in /proc/mounts
is_mounted() {
  local mountpoint="$1"
  local regexp="$2"

  local absolute_mnt=
  # Use canonicalize-missing because the mount point can be broken and
  # inaccessible
  absolute_mnt="$(readlink --canonicalize-missing $mountpoint)" || return 2
  local mnt_record="$(cat /proc/mounts 2>/dev/null | grep " $absolute_mnt ")"
  if [ x"$mnt_record" = x"" ]; then
    return 1
  fi

  [ x"$regexp" = x"" ] || echo "$mnt_record" | grep -q "$regexp"
}


run_suid_helper() {
  env -i /usr/bin/cvmfs_suid_helper $@
}


### Logging functions

to_syslog() {
  logger -t cvmfs_server "$1" > /dev/null 2>&1 || return 0
}


to_syslog_for_repo() {
  local repo_name="$1"
  local message="$2"
  to_syslog "(${repo_name}) $message"
}


__hc_print_status_report() {
  local name="$1"
  local rdonly_broken=$2
  local rdonly_outdated=$3
  local rw_broken=$4
  local rw_should_be_rdonly=$5
  local rw_should_be_rw=$6

  [ $rdonly_broken       -eq 0 ] || echo "${CVMFS_SPOOL_DIR}/rdonly is not mounted properly."                   >&2
  [ $rdonly_outdated     -eq 0 ] || echo "$name is not based on the newest published revision"                  >&2
  [ $rw_broken           -eq 0 ] || echo "/cvmfs/$name is not mounted properly."                                >&2
  [ $rw_should_be_rdonly -eq 0 ] || echo "$name is not in a transaction but /cvmfs/$name is mounted read/write" >&2
  [ $rw_should_be_rw     -eq 0 ] || echo "$name is in a transaction but /cvmfs/$name is not mounted read/write" >&2
}


__hc_transition() {
  local name="$1"
  local quiet="$2"
  local transition="$3"
  local msg=""
  local stdout_msg=""
  local retcode=0

  case $transition in
    rw_umount)     msg="Trying to umount /cvmfs/${name}"             ;;
    rdonly_umount) msg="Trying to umount ${CVMFS_SPOOL_DIR}/rdonly"  ;;
    rw_mount)      msg="Trying to mount /cvmfs/${name}"              ;;
    rdonly_mount)  msg="Trying to mount ${CVMFS_SPOOL_DIR}/rdonly"   ;;
    open)          msg="Trying to remount /cvmfs/${name} read/write" ;;
    lock)          msg="Trying to remount /cvmfs/${name} read-only"  ;;
    *)             exit 2                                            ;;
  esac

  stdout_msg="Note: ${msg}... "
  [ $quiet = 0 ] && echo -n "$stdout_msg" >&2

  if run_suid_helper $transition $name > /dev/null; then
    [ $quiet = 0 ] && echo "success" >&2
    to_syslog_for_repo $name "${msg}... success"
  else
    [ $quiet = 0 ] || echo -n "${stdout_msg}" >&2 # error messages are not quiet
    echo "fail"                               >&2
    to_syslog_for_repo $name "${msg}... fail"
    exit 1
  fi
}


### Locking functions

# Helper functions for file locking including detection of stale locks
# Note: The implementation idea was found here:
#       http://rute.2038bug.com/node23.html.gz
__is_valid_lock() {
  local path="$1"
  local ignore_stale="$2"

  local lock_file="${path}.lock"
  [ -f $lock_file ]      || return 1 # lock doesn't exist
  [ -z "$ignore_stale" ] || return 0 # lock is there (skip the stale test)

  local stale_pid=$(cat $lock_file 2>/dev/null)
  [ $stale_pid -gt 0 ]     && \
  kill -0 $stale_pid 2>/dev/null
}


acquire_lock() { # hardlink creation is guaranteed to be atomic!
  local path="$1"
  local ignore_stale="$2"

  local pid="$$"
  local temp_file="${path}.${pid}"
  local lock_file="${path}.lock"
  echo $pid > $temp_file || return 1 # probably no access to $path

  if ln $temp_file $lock_file 2>/dev/null; then
    rm -f $temp_file 2>/dev/null
    return 0 # lock acquired
  fi

  if __is_valid_lock "$path" "$ignore_stale"; then
    rm -f $temp_file 2>/dev/null
    return 1 # lock couldn't be acquired and appears valid
  fi

  rm -f $lock_file 2>/dev/null # lock was stale and can be removed
  if ln $temp_file $lock_file; then
    rm -f $temp_file 2>/dev/null
    return 0 # lock acquired
  fi

  rm -f $temp_file 2>/dev/null
  return 1 # lock couldn't be acquired after removing stale lock (lost the race)
}


wait_and_acquire_lock() {
  local path="$1"
  while ! acquire_lock "$path"; do
    sleep 5
  done
}


release_lock() {
  local path="$1"
  local lock_file="${path}.lock"
  rm -f $lock_file 2>/dev/null
}


check_lock() {
  local path="$1"
  local ignore_stale="$2"
  __is_valid_lock "${path}" "$ignore_stale"
}


# makes sure that a version is always of the form x.y.z-b
normalize_version() {
  local version_string="$1"
  while [ $(echo "$version_string" | grep -o '\.' | wc -l) -lt 2 ]; do
    version_string="${version_string}.0"
  done
  while [ $(echo "$version_string" | grep -o '-' | wc -l) -lt 1 ]; do
    version_string="${version_string}-1"
  done
  echo "$version_string"
}


# returns the version string of the current cvmfs installation
cvmfs_version_string() {
  local version_string
  if ! __swissknife version > /dev/null 2>&1; then
    # Fallback: for CernVM-FS versions before 2.1.7
    # this is just a security measure... it should never happen, since this
    # function was introduced with CernVM-FS 2.1.7
    version_string=$(__swissknife --version | sed -n '2{p;q}' | awk '{print $2}')
  else
    version_string=$(__swissknife --version)
  fi
  echo $(normalize_version $version_string)
}


version_major() { echo $1 | cut --delimiter=. --fields=1 | grep -oe '^[0-9]\+'; }
version_minor() { echo $1 | cut --delimiter=. --fields=2 | grep -oe '^[0-9]\+'; }
version_patch() { echo $1 | cut --delimiter=. --fields=3 | grep -oe '^[0-9]\+'; }
version_build() { echo $1 | cut --delimiter=- --fields=2 | grep -oe '^[0-9]\+'; }


prepend_zeros() { printf %03d "$1"; }


compare_versions() {
  local lhs="$(normalize_version $1)"
  local comparison_operator=$2
  local rhs="$(normalize_version $3)"

  local lhs1=$(prepend_zeros $(version_major $lhs))
  local lhs2=$(prepend_zeros $(version_minor $lhs))
  local lhs3=$(prepend_zeros $(version_patch $lhs))
  local lhs4=$(prepend_zeros $(version_build $lhs))
  local rhs1=$(prepend_zeros $(version_major $rhs))
  local rhs2=$(prepend_zeros $(version_minor $rhs))
  local rhs3=$(prepend_zeros $(version_patch $rhs))
  local rhs4=$(prepend_zeros $(version_build $rhs))

  [ $lhs1$lhs2$lhs3$lhs4 $comparison_operator $rhs1$rhs2$rhs3$rhs4 ]
}


version_equal() {
  local needle=$1
  compare_versions "$(cvmfs_version_string)" = "$needle"
}


version_greater_or_equal() {
  local needle=$1
  compare_versions $(cvmfs_version_string) -ge $needle
}


version_lower_or_equal() {
  local needle=$1
  compare_versions $(cvmfs_version_string) -le $needle
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


get_upstream_type() {
  local upstream=$1
  echo "$upstream" | cut -d, -f1
}


check_upstream_type() {
  local upstream=$1
  local needle_type=$2
  [ x$(get_upstream_type $upstream) = x"$needle_type" ]
}


is_local_upstream() {
  local upstream=$1
  check_upstream_type $upstream "local"
}

