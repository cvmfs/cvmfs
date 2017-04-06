#!/bin/sh

# source the common platform independent functionality and option parsing
script_location=$(dirname $(readlink --canonicalize $0))
. ${script_location}/common_setup.sh

# install CernVM-FS RPM packages
echo "installing RPM packages... "
install_rpm "$CONFIG_PACKAGES"
install_rpm $CLIENT_PACKAGE
install_rpm $SERVER_PACKAGE  # only needed for tbb shared libs (unit tests)
install_rpm $UNITTEST_PACKAGE

# setup environment
echo -n "setting up CernVM-FS environment..."
sudo sh -c "echo CVMFS_CACHE_PRIMARY=ram_extern > /etc/cvmfs/default.d/90-ramcache.conf"
sudo sh -c "echo CVMFS_CACHE_ram_extern_TYPE=external"
sudo sh -c "echo CVMFS_CACHE_ram_extern_CMDLINE=/usr/libexec/cvmfs/cache/cvmfs_cache_ram,/etc/cvmfs/default.d/90-ramcache.conf >> /etc/cvmfs/default.d/90-ramcache.conf"
sudo sh -c "echo CVMFS_CACHE_ram_extern_LOCATOR=unix=/var/lib/cvmfs/cvmfs-cache.socket >> /etc/cvmfs/default.d/90-ramcache.conf"
sudo sh -c "echo CVMFS_CACHE_PLUGIN_LOCATOR=unix=/var/lib/cvmfs/cvmfs-cache.socket >> /etc/cvmfs/default.d/90-ramcache.conf"
sudo sh -c "echo CVMFS_CACHE_PLUGIN_SIZE=1500 >> /etc/cvmfs/default.d/90-ramcache.conf"
sudo cvmfs_config setup                          || die "fail (cvmfs_config setup)"
sudo mkdir -p /var/log/cvmfs-test                || die "fail (mkdir /var/log/cvmfs-test)"
sudo chown sftnight:sftnight /var/log/cvmfs-test || die "fail (chown /var/log/cvmfs-test)"
sudo systemctl start autofs                      || die "fail (systemctl start autofs)"
sudo cvmfs_config chksetup > /dev/null           || die "fail (cvmfs_config chksetup)"
echo "done"

# install additional stuff (needed for perl testing tools)
echo "installing additional RPM packages..."
install_from_repo gcc
install_from_repo gcc-c++

# traffic shaping
install_from_repo trickle

# increase open file descriptor limits
echo -n "increasing ulimit -n ... "
set_nofile_limit 65536 || die "fail"
echo "done"
