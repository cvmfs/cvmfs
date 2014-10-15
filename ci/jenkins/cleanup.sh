#/bin/sh

echo "Continuous Integration Cleanup Script"

script_location=$(dirname $(readlink --canonicalize $0))
. ${script_location}/common.sh


cleanup_rpms() {
  local packages="$(rpm -qa | grep cvmfs)"
  echo "Packages to be deleted:"
  for package in $packages; do
    echo $package
  done
  sudo rpm -e $packages > /dev/null
}

cleanup_debs() {
  local packages="$(dpkg-query --showformat '${Package} ' --show 'cvmfs*')"
  echo "Packages to be deleted:"
  for package in $packages; do
    echo $package
  done
  sudo dpkg --purge $packages
}

echo "Cleanup Packages"
case $PACKAGE_TYPE in
  rpm)
    cleanup_rpms
    ;;
  deb)
    cleanup_debs
    ;;
  *)
    echo "FAIL: unknown package type '$PACKAGE_TYPE'"
    ;;
esac

sudo rm -fR $(pwd)/${CVMFS_BUILD_RESULTS}

sudo /usr/sbin/userdel cvmfs
sudo rm -rf /etc/cvmfs /var/cache/cvmfs2 /var/lib/cvmfs
sudo sed -i "/^\/cvmfs \/etc\/auto.cvmfs/d" /etc/auto.master
if [ -e /etc/fuse.conf ]; then
  sudo sed -i "/added by CernVM-FS/d" /etc/fuse.conf
fi
[ -f /var/lock/subsys/autofs ] && sudo /sbin/service autofs restart
sudo rm -rf /cvmfs
sudo rm -rf /usr/bin/cvmfs-* /usr/bin/cvmfs_* /sbin/mount.cvmfs /etc/auto.cvmfs /etc/init.d/cvmfs
