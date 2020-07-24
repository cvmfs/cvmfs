#!/bin/sh

BOOT_LOG=/var/log/boot.log

cleanup() {
  date | tee -a $BOOT_LOG
  echo "[INF] unmounting /cvmfs area"
  find /cvmfs -mindepth 1 -maxdepth 1 -type d -exec umount -l {} \;
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

echo "CVMFS_REPOSITORIES=$CVMFS_REPOSITORIES" > /etc/cvmfs/default.local

if [ -z "$CVMFS_HTTP_PROXY" ]; then
  echo "[ERR] CVMFS_HTTP_PROXY environment variable required" | tee -a $BOOT_LOG
  exit 1
fi
echo "[INF] using CVMFS_HTTP_PROXY='$CVMFS_HTTP_PROXY'" | tee -a $BOOT_LOG
echo "CVMFS_HTTP_PROXY='$CVMFS_HTTP_PROXY'" >> /etc/cvmfs/default.local

if [ ! -z "$CVMFS_QUOTA_LIMIT" ]; then
  echo "[INF] using CVMFS_QUOTA_LIMIT='$CVMFS_QUOTA_LIMIT'" | tee -a $BOOT_LOG
  echo "CVMFS_QUOTA_LIMIT='$CVMFS_QUOTA_LIMIT'" >> /etc/cvmfs/default.local
fi

# unmount on container exit
trap cleanup SIGTERM SIGINT

echo "[INF] mounting $(echo $CVMFS_REPOSITORIES | tr , ' ')"
for r in $(echo $CVMFS_REPOSITORIES | tr , ' '); do
  mkdir -p /cvmfs/$r
  cvmfs2 -o fsname=cvmfs2,system_mount,allow_other,grab_mountpoint $r /cvmfs/$r || exit 1
done

echo "[INF] done, entering service life cycle"
sleep infinity
