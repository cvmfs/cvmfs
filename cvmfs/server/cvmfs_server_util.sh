#
# This file is part of the CernVM File System
# This script takes care of creating, removing, and maintaining repositories
# on a Stratum 0/1 server
#
# Utility functions used by the  "cvmfs_server" script
#


mangle_local_cvmfs_url() {
  local repo_name=$1
  echo "http://localhost/cvmfs/${repo_name}"
}


mangle_s3_cvmfs_url() {
  local repo_name=$1
  local s3_url="$2"
  [ $(echo -n "$s3_url" | tail -c1) = "/" ] || s3_url="${s3_url}/"
  echo "${s3_url}${repo_name}"
}


make_upstream() {
  local type_name=$1
  local tmp_dir=$2
  local config_string=$3
  echo "$type_name,$tmp_dir,$config_string"
}


make_s3_upstream() {
  local repo_name=$1
  local s3_config=$2
  make_upstream "S3" "/var/spool/cvmfs/${repo_name}/tmp" "${repo_name}@${s3_config}"
}


make_local_upstream() {
  local repo_name=$1
  local repo_storage="${DEFAULT_LOCAL_STORAGE}/${repo_name}"
  make_upstream "local" "${repo_storage}/data/txn" "$repo_storage"
}


# checks if the aufs kernel module is present
# or if aufs is compiled in
# @return   0 if the aufs kernel module is loaded
check_aufs() {
  $MODPROBE_BIN -q aufs || test -d /sys/fs/aufs
}


# checks if the overlayfs kernel module is present
# or if overlayfs is compiled in
# @return   0 if the overlayfs kernel module is loaded
check_overlayfs() {
  $MODPROBE_BIN -q overlay || test -d /sys/module/overlay
}


# check if at least one of the supported union file systems is available
# currently AUFS get preference over OverlayFS if both are available
#
# @return   0 if at least one was found (name through stdout); abort otherwise
get_available_union_fs() {
  if check_aufs; then
    echo "aufs"
  elif check_overlayfs; then
    echo "overlayfs"
  else
    die "neither AUFS nor OverlayFS detected on the system!"
  fi
}


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


# gets the number of open writable file descriptors beneath a given path
#
# @param path  the path to look at for open writable fds
# @return      the number of open writable file descriptors
count_wr_fds() {
  local path=$1
  local cnt=0
  for line in $(get_fd_modes $path); do
    if echo "$line" | grep -qe '^\a[wu]$'; then cnt=$(( $cnt + 1 )); fi
  done
  echo $cnt
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


# returns 0 if the current working dir is somewhere under $path
#
# @param path  the path to look at
# @return      0 if cwd is on path or below, 1 otherwise
is_cwd_on_path() {
  local path=$1

  if [ "x$(pwd)" = "x${path}" ]; then
    return 0
  fi
  if echo "x$(pwd)" | grep -q "^x${path}/"; then
    return 0
  fi

  return 1
}


check_upstream_validity() {
  local upstream=$1
  local silent=0
  if [ $# -gt 1 ]; then
    silent=1;
  fi

  # checks if $upstream contains _exactly three_ comma separated data fields
  if echo $upstream | grep -q "^[^,]*,[^,]*,[^,]*$"; then
    return 0
  fi

  if [ $silent -ne 1 ]; then
    usage "The given upstream definition (-u) is invalid. Should look like:
      <spooler type> , <tmp directory> , <spooler configuration>"
  fi
  return 1
}


# ensure that the installed overlayfs is viable for CernVM-FS. Namely, it must
# be part of the upstream kernel (since 3.18) and recent enough (kernel 4.2)
# Note: More details are in CVM-835.
# @return  0 if overlayfs is installed and viable
check_overlayfs_version() {
  [ -z "$CVMFS_DONT_CHECK_OVERLAYFS_VERSION" ] || return 0
  local krnl_version=$(uname -r | grep -oe '^[0-9]\+\.[0-9]\+.[0-9]\+')
  compare_versions "$krnl_version" -ge "4.2.0"
}


# checks if cvmfs2 client is installed
#
# @return  0 if cvmfs2 client is installed
check_cvmfs2_client() {
  [ -x /usr/bin/cvmfs2 ]
}


# lowers restrictions of hardlink creation if needed
# allows AUFS to properly whiteout files without root privileges
# Note: this function requires a privileged user
lower_hardlink_restrictions() {
  if [ -f /proc/sys/kernel/yama/protected_nonaccess_hardlinks ] && \
     [ $(cat /proc/sys/kernel/yama/protected_nonaccess_hardlinks) -ne 0 ]; then
    # disable hardlink restrictions at runtime
    sysctl -w kernel.yama.protected_nonaccess_hardlinks=0 > /dev/null 2>&1 || return 1

    # change sysctl.conf to make the change persist reboots
    cat >> /etc/sysctl.conf << EOF

# added by CVMFS to allow proper whiteout of files in AUFS
# when creating or altering repositories on this machine.
kernel.yama.protected_nonaccess_hardlinks=0
EOF
    echo "Note: permanently disabled kernel option: kernel.yama.protected_nonaccess_hardlinks"
  fi

  return 0
}


_setcap_if_needed() {
  local binary_path="$1"
  local capability="$2"
  [ -x $binary_path ]                                || return 0
  $GETCAP_BIN "$binary_path" | grep -q "$capability" && return 0
  $SETCAP_BIN "${capability}+p" "$binary_path"
}


# grants CAP_SYS_ADMIN to cvmfs_swissknife if it is necessary
# Note: OverlayFS uses trusted extended attributes that are not readable by a
#       normal unprivileged process
ensure_swissknife_suid() {
  local unionfs="$1"
  local sk_bin="/usr/bin/$CVMFS_SERVER_SWISSKNIFE"
  local sk_dbg_bin="/usr/bin/${CVMFS_SERVER_SWISSKNIFE}_debug"
  local cap="cap_sys_admin"

  # check if we need CAP_SYS_ADMIN for cvmfs_swissknife...
  is_root || die "need to be root for granting CAP_SYS_ADMIN to $sk_bin"
  [ x"$unionfs" = x"overlayfs" ] || return 0

  # ... yes, obviously we need CAP_SYS_ADMIN for cvmfs_swissknife
  _setcap_if_needed "$sk_bin"     "$cap" || return 1
  _setcap_if_needed "$sk_dbg_bin" "$cap" || return 2
}


find_sbin() {
  local bin_name="$1"
  local bin_path=""
  for d in /sbin /usr/sbin /usr/local/sbin /bin /usr/bin /usr/local/bin; do
    bin_path="${d}/${bin_name}"
    if [ -x "$bin_path" ]; then
      echo "$bin_path"
      return 0
    fi
  done
  return 1
}


is_redhat() {
  [ -f /etc/redhat-release ]
}


# whenever you print the version string you should use this function since
# a repository created before CernVM-FS 2.1.7 cannot be fingerprinted
# correctly...
# @param version_string  the plain version string
mangle_version_string() {
  local version_string=$1
  if [ x"$version_string" = x"2.1.6" ]; then
    echo "2.1.6 or lower"
  else
    echo $version_string
  fi
}


# checks if a user exists in the system
#
# @param user   the name of the user to be checked
# @return       0 if user was found
check_user() {
  local user=$1
  id $user > /dev/null 2>&1
}


get_cvmfs_owner() {
  local name=$1
  local owner=$2
  local cvmfs_owner

  if [ "x$owner" = "x" ]; then
    read -p "Owner of $name [$(whoami)]: " cvmfs_owner
    [ x"$cvmfs_owner" = x ] && cvmfs_owner=$(whoami)
  else
    cvmfs_owner=$owner
  fi
  check_user $cvmfs_user || return 1
  echo $cvmfs_owner
}


is_s3_upstream() {
  local upstream=$1
  check_upstream_type $upstream "s3"
}

get_upstream_config() {
  local upstream=$1
  echo "$upstream" | cut -d, -f3-
}


has_selinux() {
  [ -x $SESTATUS_BIN   ] && \
  [ -x $GETENFORCE_BIN ] && \
  $GETENFORCE_BIN | grep -qi "enforc" || return 1
}


set_selinux_httpd_context_if_needed() {
  local directory="$1"
  if has_selinux; then
    chcon -Rv --type=httpd_sys_content_t ${directory}/ > /dev/null
  fi
}


_cleanup_tmrc() {
  local tmpdir=$1
  umount ${tmpdir}/c > /dev/null 2>&1 || umount -l > /dev/null 2>&1
  rm -fR ${tmpdir}   > /dev/null 2>&1
}


# for some reason `mount -o remount,(ro|rw) /cvmfs/$name` does not work on older
# platforms if we set the SELinux context=... parameter in /etc/fstab
# this dry-runs the whole mount, remount, unmount cycle to find out if it works
# correctly (aufs version)
# @returns  0 if the whole cycle worked as expected
try_mount_remount_cycle_aufs() {
  local tmpdir
  tmpdir=$(mktemp -d)
  mkdir ${tmpdir}/a ${tmpdir}/b ${tmpdir}/c
  mount -t aufs \
    -o br=${tmpdir}/a=ro:${tmpdir}/b=rw,ro,context=system_u:object_r:default_t:s0 \
    try_remount_aufs ${tmpdir}/c  > /dev/null 2>&1 || return 1
  mount -o remount,rw ${tmpdir}/c > /dev/null 2>&1 || { _cleanup_tmrc $tmpdir; return 2; }
  mount -o remount,ro ${tmpdir}/c > /dev/null 2>&1 || { _cleanup_tmrc $tmpdir; return 3; }
  _cleanup_tmrc $tmpdir
  return 0
}


print_new_repository_notice() {
  local name=$1
  local cvmfs_user=$2

  echo "\

Before you can install anything, call \`cvmfs_server transaction\`
to enable write access on your repository. Then install your
software in /cvmfs/$name as user $cvmfs_user.
Once you're happy, publish using \`cvmfs_server publish\`

For client configuration, have a look at 'cvmfs_server info'

If you go for production, backup you software signing keys in /etc/cvmfs/keys/!"
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

