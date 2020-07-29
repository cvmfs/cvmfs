#!/bin/sh

# Force unmount of cvmfs volumes (triggered by k8s pre-stop)

BOOT_LOG=/var/log/boot.log

date | tee -a $BOOT_LOG
echo "[INF] unmounting /cvmfs area" | tee -a $BOOT_LOG
find /cvmfs -mindepth 1 -maxdepth 1 -type d -exec umount -l {} \;
