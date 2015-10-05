#!/bin/sh

if [ -f /bin/rpm ]; then
  echo -n "removing packages: cvmfs* ... "
  sudo rpm -e $(rpm -qa | grep cvmfs) > /dev/null
  if [ $? -ne 0 ]; then
    echo "fail"
    exit 3
  fi
  echo "done"
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
