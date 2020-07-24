#!/bin/sh

BOOT_LOG=/var/log/boot.log

cleanup() {
  date | tee -a $BOOT_LOG
  echo "[INF] unmounting /cvmfs area"
  find /cvmfs -mindepth 1 -maxdepth 1 -type d -exec umount -l {} \;
}

CVMFS_REPOSITORIES="cvmfs-config.cern.ch,$CVMFS_REPOSITORIES"
echo "CernVM-FS service container version " | tee -a $BOOT_LOG
date | tee -a $BOOT_LOG
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
