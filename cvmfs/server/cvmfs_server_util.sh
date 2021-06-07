#
# This file is part of the CernVM File System
# This script takes care of creating, removing, and maintaining repositories
# on a Stratum 0/1 server
#
# Utility functions used by the  "cvmfs_server" script
#

parse_url() {
  local input_url=$1
  local key=$2

  local proto host port path url

  local has_proto=$(echo $input_url | grep '://')
  if [ x"$has_proto" != x"" ]; then
      proto=$(echo $input_url | cut -d':' -f1)
      url=$(echo $input_url | cut -d'/' -f3-)
  else
      url=$input_url
  fi

  local has_path=$(echo $url | grep '/')
  if [ x"$has_path" != x"" ]; then
      path=$(echo $url | cut -d'/' -f2-)
      url=$(echo $url | cut -d'/' -f1)
  fi

  local has_port=$(echo $url | grep ':')
  if [ x"$has_port" != x"" ]; then
      port=$(echo $url | cut -d':' -f2)
      host=$(echo $url | cut -d':' -f1)
  else
      host=$url
  fi

  case $key in
    proto)
        echo "$proto"
        ;;
    host)
        echo "$host"
        ;;
    port)
        echo "$port"
        ;;
    path)
        echo "$path"
        ;;
    *)
        echo "proto: $proto"
        echo "host: $host"
        echo "port: $port"
        echo "path: $path"
  esac
}


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
  local subpath=$3
  local repo_alias
  local disable_dns_buckets=$(cat $s3_config | grep "CVMFS_S3_DNS_BUCKETS=false")
  if [ x"$disable_dns_buckets" = x"" ] && [ x"$subpath" != x"" ]; then
    repo_alias=$subpath/$repo_name
  else
    repo_alias=$repo_name
  fi
  make_upstream "S3" "/var/spool/cvmfs/${repo_name}/tmp" "${repo_alias}@${s3_config}"
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
# currently OverlayFS get preference over AUFS if both are available
#
# @return   0 if at least one was found (name through stdout); abort otherwise
get_available_union_fs() {
  if check_overlayfs; then
    echo "overlayfs"
  elif check_aufs; then
    echo "aufs"
  else
    die "neither OverlayFS nor AUFS detected on the system!"
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


__publish_cmd() {
  local might_be_debugging="$1"
  if [ ! -z $might_be_debugging ]; then
    echo "$CVMFS_SERVER_PUBLISH_DEBUG"
  else
    echo "$CVMFS_SERVER_PUBLISH"
  fi
}


__publish() {
  $(__publish_cmd) $@
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
  ! cvmfs_sys_file_is_regular /etc/cvmfs/repositories.d/$name/replica.conf
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
  local mnt_record="$(cat /proc/mounts 2>/dev/null | grep -F " $absolute_mnt ")"
  if [ x"$mnt_record" = x"" ]; then
    return 1
  fi

  [ x"$regexp" = x"" ] || echo "$mnt_record" | grep -q "$regexp"
}


# only certain characters are allowed in repository names
#
# @param repo_name the name to test
is_valid_repo_name() {
  local repo_name="$1"

  [ ! -z "$1" ] || return 1
  local length=$(echo -n "$repo_name" | wc -c)
  [ $length -le 60 ] || return 1

  local repo_head="$(echo "$repo_name" | head -c 1)"
  local clean_head="$(echo "$repo_head" | tr -cd a-zA-Z0-9)"
  [ "x$clean_head" = "x$repo_head" ] || return 1

  local clean_name=$(echo "$repo_name" | tr -cd a-zA-Z0-9_.-)
  [ "x$clean_name" = "x$repo_name" ]
}


# only certain characters are allowed in branch names
#
# @param branch_name the name to test
is_valid_branch_name() {
  local branch_name="$1"

  local clean_name=$(echo "$branch_name" | tr -cd a-zA-Z0-9_@./-)
  [ "x$clean_name" = "x$branch_name" ]
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
  local rdonly_wronghash=$4
  local rw_broken=$5
  local rw_should_be_rdonly=$6
  local rw_should_be_rw=$7

  [ $rdonly_broken       -eq 0 ] || echo "${CVMFS_SPOOL_DIR}/rdonly is not mounted properly."                   >&2
  [ $rdonly_outdated     -eq 0 ] || echo "$name is not based on the newest published revision"                  >&2
  [ $rdonly_wronghash    -eq 0 ] || echo "$name is not based on the checked out revision"                       >&2
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
  cvmfs_sys_file_is_regular $lock_file || return 1 # lock doesn't exist
  [ -z "$ignore_stale" ] || return 0 # lock is there (skip the stale test)

  local stale_pid=$(cat $lock_file 2>/dev/null)
  [ -n "$stale_pid" ] && [ $stale_pid -gt 0 ]     && \
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

# Tracks changes to the organization of files and directories.
# Stored in CVMFS_CREATOR_VERSION.  Started with 137.
cvmfs_layout_revision() { echo "142"; }

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


# Ensure that the installed overlayfs is viable for CernVM-FS.
# Note: More details are in CVM-835.
# @return  0 if overlayfs is installed and viable
#          1 if it is not viable, and stdout contains a reason
# This should probably now be called check_overlayfs_viability except
#   that for backward compatiblity we need to keep the variable that
#   overrides it, CVMFS_DONT_CHECK_OVERLAYFS_VERSION, and changing the
#   function name would make the variable name not make sense.
check_overlayfs_version() {
  if ! check_overlayfs; then
    echo "overlayfs kernel module missing"
    return 1
  fi
  [ -z "$CVMFS_DONT_CHECK_OVERLAYFS_VERSION" ] || return 0
  local krnl_version=$(cvmfs_sys_uname)
  local required_version="4.2.0"
  if compare_versions "$krnl_version" -ge "$required_version" ; then
    return 0
  fi
  if cvmfs_sys_is_redhat; then
    # Redhat kernel with backported overlayfs supports limited filesystem types
    required_version="3.10.0-493"
    if compare_versions "$krnl_version" -ge "$required_version" ; then
      # If the mounted filesystem name is long df will split output into two
      #  lines, so use tail -n +2 to skip first line and echo to combine them
      local scratch_fstype=$(echo $(df -T /var/spool/cvmfs | tail -n +2) | awk {'print $2'})
      if [ "x$scratch_fstype" = "xext3" ] || [ "x$scratch_fstype" = "xext4" ] ; then
        return 0
      fi
      if [ "x$scratch_fstype" = "xxfs" ] ; then
        if [ "x$(xfs_info /var/spool/cvmfs 2>/dev/null | grep ftype=1)" != "x" ] ; then
          return 0
        else
          echo "XFS with ftype=0 is not supported for /var/spool/cvmfs. XFS with ftype=1 is required"
          return 1
        fi
      fi
      echo "overlayfs scratch /var/spool/cvmfs is type $scratch_fstype, but ext3, ext4 or xfs(ftype=1) required"
      return 1
    fi
  fi
  echo "Kernel version $krnl_version too old for overlayfs; at least $required_version required"
  return 1
}


# checks if cvmfs2 client is installed
#
# @return  0 if cvmfs2 client is installed
check_cvmfs2_client() {
  cvmfs_sys_file_is_executable /usr/bin/cvmfs2
}


# lowers restrictions of hardlink creation if needed
# allows AUFS to properly whiteout files without root privileges
# Note: this function requires a privileged user
lower_hardlink_restrictions() {
  if cvmfs_sys_file_is_regular /proc/sys/kernel/yama/protected_nonaccess_hardlinks && \
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
  cvmfs_sys_file_is_executable $binary_path || return 0
  $SETCAP_BIN -v "${capability}" "$binary_path" >/dev/null 2>&1 && return 0
  $SETCAP_BIN "${capability}" "$binary_path"
}


# grants CAP_SYS_ADMIN to cvmfs_swissknife and cvmfs_publish if it is necessary
# Note: OverlayFS uses trusted extended attributes that are not readable by a
#       normal unprivileged process
ensure_swissknife_suid() {
  local unionfs="$1"
  local sk_bin="/usr/bin/$CVMFS_SERVER_SWISSKNIFE"
  local sk_dbg_bin="/usr/bin/${CVMFS_SERVER_SWISSKNIFE}_debug"
  local pb_bin="/usr/bin/cvmfs_publish"
  local pb_dbg_bin="/usr/bin/cvmfs_publish_debug"
  local cap_read="cap_dac_read_search"
  local cap_overlay="cap_sys_admin"

  is_root || die "need to be root for granting capabilities to $sk_bin"

  if [ x"$unionfs" = x"overlayfs" ]; then
    _setcap_if_needed "$sk_bin"     "${cap_read},${cap_overlay}+p" || return 3
    _setcap_if_needed "$sk_dbg_bin" "${cap_read},${cap_overlay}+p" || return 4
    _setcap_if_needed "$pb_bin"     "${cap_read},${cap_overlay}+p" || return 5
    _setcap_if_needed "$pb_dbg_bin" "${cap_read},${cap_overlay}+p" || return 6
  else
    _setcap_if_needed "$sk_bin"     "${cap_read}+p" || return 1
    _setcap_if_needed "$sk_dbg_bin" "${cap_read}+p" || return 2
    _setcap_if_needed "$pb_bin"     "${cap_read}+p" || return 7
    _setcap_if_needed "$pb_dbg_bin" "${cap_read}+p" || return 8
  fi
}


find_sbin() {
  local bin_name="$1"
  local bin_path=""
  for d in /sbin /usr/sbin /usr/local/sbin /bin /usr/bin /usr/local/bin; do
    bin_path="${d}/${bin_name}"
    if cvmfs_sys_file_is_executable "$bin_path" ; then
      echo "$bin_path"
      return 0
    fi
  done
  return 1
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
  cvmfs_sys_file_is_executable $SESTATUS_BIN && \
  cvmfs_sys_file_is_executable $GETENFORCE_BIN && \
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
  local skip_backup_notice=$3

  echo "\

Before you can install anything, call \`cvmfs_server transaction\`
to enable write access on your repository. Then install your
software in /cvmfs/$name as user $cvmfs_user.
Once you're happy, publish using \`cvmfs_server publish\`

For client configuration, have a look at 'cvmfs_server info'"
  if [ $skip_backup_notice -eq 0 ]; then
    echo "\

If you go for production, backup your masterkey from /etc/cvmfs/keys/!"
  fi
}


get_fd_modes() {
  local path=$1
  $LSOF_BIN -Fan +f -- $path 2>/dev/null | grep -B1 -e "^n$path" | grep -e '^a.*'
}

# gets the number of open read-only file descriptors beneath a given path
#
# @param path  the path to look at for open read-only fds
# @return      the number of open read-only file descriptors
count_rd_only_fds() {
  local path=$1
  local cnt=0
  for line in $(get_fd_modes $path); do
    if echo "$line" | grep -qe '^\ar\?$';  then cnt=$(( $cnt + 1 )); fi
  done
  echo $cnt
}

# find the partition name for a given file path
#
# @param   path  the path to the file to be checked
# @return  the name of the partition that path resides on
get_partition_for_path() {
  local path="$1"
  df --portability "$path" | tail -n1 | awk '{print $1}'
}


# checks if a given repository is replicable
#
# @param name   the repository name or URL to be checked
# @return       0 if it is a stratum0 repository and replicable
is_master_replica() {
  local name=$1
  local is_master_replica

  if [ $(echo $name | cut --bytes=1-7) = "http://" ]; then
    is_master_replica=$(get_repo_info_from_url $name -m -L)
  else
    load_repo_config $name
    is_stratum0 $name || return 1
    is_master_replica=$(get_repo_info -m)
  fi

  [ "x$is_master_replica" = "xtrue" ]
}


# get the path of the file that contains the content hash of the reference log
#
# @param name  the name of the repository to be checked
# @return      the full path name
get_reflog_checksum() {
  local name=$1

  echo "/var/spool/cvmfs/${name}/reflog.chksum"
}


# checks if a the reflog checksum is present in the spool directory
#
# @param name  the name of the repository to be checked
# @return      0 if the reflog checksum is available
has_reflog_checksum() {
  local name=$1

  cvmfs_sys_file_is_regular $(get_reflog_checksum $name)
}


# Find the service binary (or detect systemd)
minpidof() {
  $PIDOF_BIN $1 | tr " " "\n" | sort --numeric-sort | head -n1
}


is_systemd() {
  [ x"$SERVICE_BIN" = x"false" ]
}


# this strips both the attached signature block and the certificate hash from
# an already signed manifest file and prints the result to stdout
strip_manifest_signature() {
  local signed_manifest="$1"
  # print lines starting with a capital letter (except X for the certificate)
  # and stop as soon as we find the signature delimiter '--'
  awk '/^[A-WY-Z]/ {print $0}; /--/ {exit}' $signed_manifest
}


_update_geodb_days_since_update() {
  local timestamp=$(date +%s)
  local dbdir=$CVMFS_UPDATEGEO_DIR
  local db_mtime=$(stat --format='%Y' ${dbdir}/${CVMFS_UPDATEGEO_DB})
  local days_since_update=$(( ( $timestamp - $db_mtime ) / 86400 ))
  echo "$days_since_update"
}

_update_geodb_lazy_install_slot() {
  [ "`date +%w`" -eq "$CVMFS_UPDATEGEO_DAY"  ] && \
  [ "`date +%k`" -ge "$CVMFS_UPDATEGEO_HOUR" ]
}

_update_geodb_lazy_attempted_today() {
  local attemptdayfile="${CVMFS_UPDATEGEO_DIR}/.last_attempt_day"
  local today="`date +%j`"
  if [ "`cat $attemptdayfile 2>/dev/null`" = "$today" ]; then
    return 0
  fi
  echo "$today" >$attemptdayfile
  return 1
}

_to_syslog_for_geoip() {
  to_syslog "(GeoIP) $1"
}

_update_geodb_install() {
  local retcode=0
  local datname="$2"
  local dburl="${CVMFS_UPDATEGEO_URLBASE}?edition_id=${CVMFS_UPDATEGEO_DB%.*}&suffix=tar.gz&license_key=$CVMFS_GEO_LICENSE_KEY"
  local dbfile="${CVMFS_UPDATEGEO_DIR}/${CVMFS_UPDATEGEO_DB}"
  local download_target=${dbfile}.tgz
  local untar_dir=${dbfile}.untar

  if [ -z "$CVMFS_GEO_LICENSE_KEY" ]; then
      echo "CVMFS_GEO_LICENSE_KEY not set" >&2
      _to_syslog_for_geoip "CVMFS_GEO_LICENSE_KEY not set"
      return 1
  fi

  _to_syslog_for_geoip "started update from $dburl"

  # downloading the GeoIP database file
  curl -sS  --connect-timeout 10 \
            --max-time 60        \
            "$dburl" > $download_target || true
  if ! tar tzf $download_target >/dev/null 2>&1; then
    local msg
    if file $download_target|grep -q "ASCII text$"; then
      msg="`cat -v $download_target|head -1`"
    else
      msg="file not valid tarball"
    fi
    echo "failed to download geodb (see url in syslog): $msg" >&2
    _to_syslog_for_geoip "failed to download from $dburl: $msg"
    rm -f $download_target
    return 1
  fi

  # untar the GeoIP database file
  rm -rf $untar_dir
  mkdir -p $untar_dir
  if ! tar xmf $download_target -C $untar_dir --no-same-owner 2>/dev/null; then
    echo "failed to untar $download_target into $untar_dir" >&2
    _to_syslog_for_geoip "failed to untar $download_target into $untar_dir"
    rm -rf $download_target $untar_dir
    return 2
  fi

  # get rid of the tarred GeoIP database
  rm -f $download_target

  # atomically install the GeoIP database
  if ! mv -f $untar_dir/*/${CVMFS_UPDATEGEO_DB} $dbfile; then
    echo "failed to install $dbfile" >&2
    _to_syslog_for_geoip "failed to install $dbfile"
    rm -rf $untar_dir
    return 3
  fi

  # get rid of other files in the untar
  rm -rf $untar_dir

  set_selinux_httpd_context_if_needed "$CVMFS_UPDATEGEO_DIR"

  _to_syslog_for_geoip "successfully updated from $dburl"

  return 0
}

_update_geodb() {
  local dbdir=$CVMFS_UPDATEGEO_DIR
  local dbfile=$dbdir/$CVMFS_UPDATEGEO_DB
  local lazy=false
  local retcode=0

  # parameter handling
  OPTIND=1
  while getopts "l" option; do
    case $option in
      l)
        lazy=true
      ;;
      ?)
        shift $(($OPTIND-2))
        usage "Command update-geodb: Unrecognized option: $1"
      ;;
    esac
  done

  # sanity checks
  [ -w "$dbdir"  ]   || { echo "Directory '$dbdir' doesn't exist or is not writable by $(whoami)" >&2; return 1; }

  # check if an update/installation needs to be done
  if [ -z "$CVMFS_GEO_DB_FILE" ] && [ -z "$CVMFS_GEO_LICENSE_KEY" ] && \
      [ -r /usr/share/GeoIP/$CVMFS_UPDATEGEO_DB ]; then
    # Use the default location of geoipupdate
    CVMFS_GEO_DB_FILE=/usr/share/GeoIP/$CVMFS_UPDATEGEO_DB
  fi
  if [ -n "$CVMFS_GEO_DB_FILE" ]; then
    # This overrides the update/install; link to the given file instead.
    if [ ! -L "$dbfile" ] || [ "`readlink $dbfile`" != "$CVMFS_GEO_DB_FILE" ]; then
      if [ "$CVMFS_GEO_DB_FILE" != "NONE" ] && [ ! -r "$CVMFS_GEO_DB_FILE" ]; then
        echo "$CVMFS_GEO_DB_FILE doesn't exist or is not readable" >&2
        return 1
      fi
      rm -f $dbfile
      echo "Linking GeoIP Database"
      _to_syslog_for_geoip "linking db from $CVMFS_GEO_DB_FILE"
      ln -s $CVMFS_GEO_DB_FILE $dbfile
    fi
    return 0
  elif [ ! -f "$dbfile" ] || [ -L "$dbfile" ]; then
    echo -n "Installing GeoIP Database... "
  elif ! $lazy; then
    echo -n "Updating GeoIP Database... "
  else
    local days_old=$(_update_geodb_days_since_update)
    if [ $days_old -gt $CVMFS_UPDATEGEO_MAXDAYS ]; then
      if _update_geodb_lazy_attempted_today; then
        # already attempted today, wait until tomorrow
        return 0
      fi
      echo -n "GeoIP Database is very old. Updating... "
    elif [ $days_old -gt $CVMFS_UPDATEGEO_MINDAYS ]; then
      if _update_geodb_lazy_install_slot; then
        if _update_geodb_lazy_attempted_today; then
          # already attempted today, wait until next week
          return 0
        fi
        echo -n "GeoIP Database is expired. Updating... "
      else
        echo "GeoIP Database is expired, but waiting for install time slot."
        return 0
      fi
    else
      return 0
    fi
  fi

  # at this point the database needs to be installed or updated
  _update_geodb_install && echo "done" || { echo "fail"; return 3; }
}

cvmfs_server_update_geodb() {
  _update_geodb $@
}


# checks if the given command name is a supported command of cvmfs_server
#
# @param subcommand   the subcommand to be called
# @return   0 if the command was recognized
is_subcommand() {
  local subcommand="$1"
  local supported_commands="mkfs add-replica import publish rollback rmfs alterfs   \
    resign list info tag list-tags lstags check transaction enter abort snapshot    \
    skeleton migrate list-catalogs diff checkout update-geodb gc catalog-chown      \
    eliminate-hardlinks eliminate-bulk-hashes fix-stats update-info update-repoinfo \
    mount fix-permissions masterkeycard ingest merge-stats print-stats"

  for possible_command in $supported_commands; do
    if [ x"$possible_command" = x"$subcommand" ]; then
      return 0
    fi
  done

  return 1
}


# Flushes data to disk; only flush if the enforced level allowed for the
# provided level.  The order is
#  'none' (never sync)
#  'default' (sync for rare, important operations like mkfs, publish)
#  'cautious' (always sync)
syncfs() {
  local level="${1:-default}"
  local enforced_level="${CVMFS_SYNCFS_LEVEL:-default}"
  [ "x$enforced_level" = "xnone" ] && return || true
  [ "x$enforced_level" = "xdefault" -a "x$level" = "xcautious" ] && return || true

  sync
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
                  [-R require masterkeycard key ]
                  [-V VOMS authorization] [-X (external data)]
                  <fully qualified repository name>
                  Creates a new repository with a given name
  add-replica     [-u stratum1 upstream storage] [-o owner] [-w stratum1 url]
                  [-a silence apache warning] [-z enable garbage collection]
                  [-n alias name] [-s S3 config file] [-p no apache config]
                  [-g snapshot group] [-P pass-through repository]
                  <stratum 0 url> <public key | keys directory>
                  Creates a Stratum 1 replica of a Stratum 0 repository
  import          [-w stratum0 url] [-o owner] [-u upstream storage]
                  [-l import legacy repo (2.0.x)] [-s show migration statistics]
                  [-f union filesystem type] [-c file ownership (UID:GID)]
                  [-k path to keys] [-g chown backend] [-r recreate whitelist]
                  [-p no apache config] [-t recreate repo key and certificate]
                  [ -R recreate whitelist and require masterkeycard ]
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
                  [-t time stamp after which revisions are preserved]
                  [-l print deleted objects] [-L log of deleted objects]
                  [-f force] [-d dry run]
                  [-a collect all garbage-collectable repos, log to gc.log |
                    <fully qualified name> ]
                  Remove unreferenced data from garbage-collectable repository
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
  resign          [ -w path to existing whitelist ]
                  [ -d days until expiration (default 30) ]
                  [ -f don't ask again ]
                  <fully qualified name>
                  Re-sign the whitelist.
                  Default expiration days goes down to 7 with masterkeycard.
  resign -p       <fully qualified name>
                  Re-sign .cvmfspublished
  masterkeycard   -a Checks if a smartcard is available
                  -k Checks whether a key is stored in a card
                  -r Reads pub key from a card to stdout
                  [ -f don't ask again ] -s <fully qualified name>
                     Stores masterkey and pub key of a repository into a card
                  [ -f don't ask again ] -d
                     Deletes a masterkey's certificate (pub key) from a card
                  [ -f don't ask again ] -c <fully qualified name or wildcard>
                     Converts given repositories to use card for whitelist
  list-catalogs   [-s catalog sizes] [-e catalog entry counts]
                  [-h catalog hashes] [-x machine readable]
                  <fully qualified name>
                  Print a full list of all nested catalogs of a repository
  diff            [-m(achine readable)] [-h(eader line)]
                  [-s <source tag>] [-d <destination tag>]
                  <fully qualified name>
                  Show change set between two snapshots (default: last publish)
  info            <fully qualified name>
                  Print summary about the repository
  tag             Create and manage named snapshots
                  [-a create tag <name>] [-c channel] [-m message] [-h hash]
                  [-r remove tag <name>] [-f don't ask again]
                  [-i inspect tag <name>] [-x machine readable]
                  [-b list branch hierarchy] [-x machine readable]
                  [-l list tags] [-x machine readable]
                  <fully qualified name>
                  Print named tags (snapshots) of the repository
  checkout        [-t <tag name>] [-b <branch name>]
                  <fully qualified name>
  check           [-c disable data chunk existence check]
                  [-i check data integrity] (may take some time)]
                  [-t tag (check given tag instead of trunk)]
                  [-s path to nested catalog subtree to check]
                  [-r repair reflog problems]
                  <fully qualified name>
                  Checks if the repository is sane
  transaction     [-r (retry if unable to acquire lease]
                  [-T /template-from=/template-to]
                  <fully qualified name>
                  Start to edit a repository
  snapshot        [-t fail if other snapshot is in progress]
                  <fully qualified name>
                  Synchronize a Stratum 1 replica with the Stratum 0 source
  snapshot -a     [-s use separate logs in /var/log/cvmfs for each repository]
                  [-n do not warn if /etc/logrotate.d/cvmfs does not exist]
                  [-i skip repositories that have not run initial snapshot]
                  [-g group (do only the repositories in snapshot group)]
                  Do snapshot on all active replica repositories
  mount           [-a | <fully qualified name>]
                  Mount repositories in /cvmfs, for instance after reboot
  migrate         <fully qualified name>
                  Migrates a repository to the current version of CernVM-FS
  catalog-chown   <-u uid map file> <-g gid map file> <fully qualified name>
                  Bulk change of the ownership ids in CernVM-FS catalogs
  list            List available repositories
  update-geodb    [-l update lazily based on CVMFS_UPDATEGEO* variables]
                  Updates the geo-IP database
  update-info     [-p no apache config] [-e don't edit /info/meta]
                  Open meta info JSON file for editing
  update-repoinfo [-f path to JSON file]
                  <fully qualified name>
                  Open repository meta info JSON file for editing
  ingest          -t tarfile
                  -b base directory
                  [-d <folder to delete>]
                  [-c create nested catalog in base directory]
                  <fully qualified name>
                  Extract the content of the tarfile inside the base directory,
                  in the same transaction it also delete the required folders.
                  Use '-' as -t argument to read the tarball from STDIN.
  print-stats     [-o output_file]
                  [-t table_name]
                  [-s separator] - char
                  <fully qualified name>
                  Print statistics values for a table (default publish_statistics)
                  using the separator specified. (default  '|')
  merge-stats     [-o output db file]
                  <db_file_1> <db_file_2>
                  Merge tables from two database files.
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

