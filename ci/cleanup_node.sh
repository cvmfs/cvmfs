#!/bin/sh

if [ -f /bin/rpm ]; then
  sudo rpm -e cvmfs-auto-setup || true
  sudo rpm -e cvmfs-init-scripts || true
  sudo rpm -e cvmfs-replica || true
  sudo rpm -e cvmfs-selinux || true
  sudo rpm -e cvmfs || true
  sudo rpm -e cvmfs-keys || true
fi
sudo /usr/sbin/userdel cvmfs
sudo rm -rf /etc/cvmfs /var/cache/cvmfs2 /var/lib/cvmfs
sudo sed -i "/^\/cvmfs \/etc\/auto.cvmfs/d" /etc/auto.master
if [ -e /etc/fuse.conf ]; then
  sudo sed -i "/added by CernVM-FS/d" /etc/fuse.conf
fi
[ -f /var/lock/subsys/autofs ] && sudo /sbin/service autofs restart
sudo rm -rf /cvmfs
sudo rm -rf /usr/bin/cvmfs-* /usr/bin/cvmfs_* /sbin/mount.cvmfs /etc/auto.cvmfs /etc/init.d/cvmfs
