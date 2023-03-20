#!/bin/sh

BOOT_LOG=/var/log/boot.log

cleanup() {
  date | tee -a $BOOT_LOG
  echo "[INF] unmounting /cvmfs area" | tee -a $BOOT_LOG
  find /cvmfs -mindepth 1 -maxdepth 1 -type d -exec umount -l {} \;
  exit 0
}

CVMFS_REPOSITORIES="cvmfs-config.cern.ch,$CVMFS_REPOSITORIES"
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
echo "                                      NOTE                                         "
echo "==================================================================================="
echo "You should run me on Docker like this"
echo "  docker run --cap-add SYS_ADMIN --device /dev/fuse --volume /cvmfs:/cvmfs:shared \\"
echo "    -e CVMFS_HTTP_PROXY=<site squid> cvmfs/service"
echo
echo "Optionally you can also set the 'CVMFS_REPOSITORIES=unpacked.cern.ch,...'"
echo "  and 'CVMFS_QUOTA_LIMIT=<cache limit in MB>' environment variables"
echo
echo "For even more control, you can bind mount (-v option) a config directory"
echo "over the container's default /etc/cvmfs and bind mount a host location"
echo "for the cache over /var/lib/cvmfs"
echo "==================================================================================="

CONFIG=/etc/cvmfs/default.d/95-container-local.conf

echo "CVMFS_REPOSITORIES=$CVMFS_REPOSITORIES" > $CONFIG

if [ -z "$CVMFS_HTTP_PROXY" -a -z "$CVMFS_CLIENT_PROFILE" ]; then
  echo "[ERR] CVMFS_HTTP_PROXY environment variable required" | tee -a $BOOT_LOG
  exit 1
fi
if [ -n "$CVMFS_HTTP_PROXY" ]; then
  echo "[INF] using CVMFS_HTTP_PROXY='$CVMFS_HTTP_PROXY'" | tee -a $BOOT_LOG
  echo "CVMFS_HTTP_PROXY='$CVMFS_HTTP_PROXY'" >> $CONFIG
fi
if [ -n "$CVMFS_CLIENT_PROFILE" ]; then
  echo "[INF] using CVMFS_CLIENT_PROFILE='$CVMFS_CLIENT_PROFILE'" | tee -a $BOOT_LOG
  echo "CVMFS_CLIENT_PROFILE='$CVMFS_CLIENT_PROFILE'" >> $CONFIG
fi

if [ ! -z "$CVMFS_QUOTA_LIMIT" ]; then
  echo "[INF] using CVMFS_QUOTA_LIMIT='$CVMFS_QUOTA_LIMIT'" | tee -a $BOOT_LOG
  echo "CVMFS_QUOTA_LIMIT='$CVMFS_QUOTA_LIMIT'" >> $CONFIG
fi

# TODO(jblomer): add all CVMFS_* environment variables to $CONFIG

# Gracefully unmount on container exit to avoid the error message
# "transport endpoint not connected" on /cvmfs/* directories
trap cleanup SIGTERM SIGINT SIGQUIT SIGHUP

echo "[INF] mounting $(echo $CVMFS_REPOSITORIES | tr , ' ')"
for r in $(echo $CVMFS_REPOSITORIES | tr , ' '); do
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
