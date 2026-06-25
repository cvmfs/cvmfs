#!/bin/sh

# Container entrypoint. Two modes, selected by CVMFS_REPOSITORIES:
#
#   unset  -> automount mode: launch automount-cvmfs, which mounts any
#             repository on first access under /cvmfs.
#   set    -> explicit mode: pre-mount each listed repository via cvmfs2
#             at startup (legacy behaviour). cvmfs-config.cern.ch is
#             always prepended.

BOOT_LOG=/var/log/boot.log

unmount_cvmfs_dirs() {
  find /cvmfs -mindepth 1 -maxdepth 1 -type d -exec umount -l {} \;
}

cleanup() {
  date | tee -a $BOOT_LOG
  echo "[INF] unmounting /cvmfs area" | tee -a $BOOT_LOG
  unmount_cvmfs_dirs
  exit 0
}

echo "CernVM-FS service container version $VERSION" | tee -a $BOOT_LOG
date | tee -a $BOOT_LOG

echo "==================================================================================="
echo
echo "                 ____             __     ____  __       _____ ____                 "
echo "                / ___|___ _ __ _ _\ \   / /  \/  |     |  ___/ ___|                "
echo "               | |   / _ \ '__| '_ \ \ / /| |\/| |_____| |_  \___ \                "
echo "               | |__|  __/ |  | | | \ V / | |  | |_____|  _|  ___) |               "
echo "                \____\___|_|  |_| |_|\_/  |_|  |_|     |_|   |____/                "
echo
echo "==================================================================================="

if [ -z "$CVMFS_HTTP_PROXY" -a -z "$CVMFS_CLIENT_PROFILE" ]; then
  echo "[ERR] CVMFS_HTTP_PROXY environment variable required" | tee -a $BOOT_LOG
  exit 1
fi

CONFIG=/etc/cvmfs/default.d/95-container-local.conf

#  add all CVMFS_* environment variables to $CONFIG
env | grep -E '^CVMFS_.+=.+$' | while IFS= read -r line; do
  echo "[INF] using $line" | tee -a $BOOT_LOG
  echo "$line" >> $CONFIG
done

# Optional cvmfs_fsck of an existing cache before mounting anything
if [ "${CVMFS_CONTAINER_RUN_FSCK}" = 'y' ] || [ "${CVMFS_CONTAINER_RUN_FSCK}" = 'Y' ]; then
  : "${CVMFS_CACHE_BASE:?CVMFS_CACHE_BASE must be set and non-empty}"
  echo "[INF] running cvmfs_fsck -p ${CVMFS_CACHE_BASE}/shared"
  /usr/bin/cvmfs_fsck -p "${CVMFS_CACHE_BASE}/shared"
fi

if [ -z "$CVMFS_REPOSITORIES" ]; then
  # ---- automount mode ----
  echo "[INF] CVMFS_REPOSITORIES unset; starting automount-cvmfs" | tee -a $BOOT_LOG
  # automount-cvmfs gracefully unmounts everything under /cvmfs on SIGTERM,
  # so no shell-level trap is required here. exec to PID 1 so signals reach
  # the daemon directly.
  exec /usr/sbin/automount-cvmfs --foreground --verbose
else
  # ---- explicit mode (legacy) ----
  # cvmfs-config.cern.ch supplies the public configuration and is always
  # mounted; prepend it. Deduplicate the list so a user who also lists it (or
  # repeats any repo) in CVMFS_REPOSITORIES does not trigger a fatal double
  # mount of the same repository on the same mountpoint (grab_mountpoint fails
  # on the second attempt and cvmfs2 exits non-zero).
  repos=""
  for r in cvmfs-config.cern.ch $(echo "$CVMFS_REPOSITORIES" | tr , ' '); do
    case " $repos " in
      *" $r "*) continue ;;
    esac
    repos="${repos:+$repos }$r"
  done
  trap cleanup SIGTERM SIGINT SIGQUIT SIGHUP
  echo "[INF] mounting $repos" | tee -a $BOOT_LOG
  for r in $repos; do
    mkdir -p /cvmfs/$r 2>/dev/null
    # Gracefully recover from ungraceful previous shutdowns
    if ls /cvmfs/$r 2>&1 | grep -q "not connected$"; then
      echo "[WARN] unmounting stale /cvmfs/$r"
      umount /cvmfs/$r
    fi
    /usr/bin/cvmfs2 -o fsname=cvmfs2,system_mount,allow_other,grab_mountpoint \
      $r /cvmfs/$r || exit 1
  done

  echo "[INF] done mounting, entering service life cycle"
  # TODO(jblomer): figure out how the script can receive the TERM signal when
  # using sleep infinity
  while true; do
    sleep 1
  done
fi
