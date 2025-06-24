#!/bin/sh

# source the common platform independent functionality and option parsing
script_location=$(dirname $(readlink --canonicalize $0))
. ${script_location}/common_setup.sh

sudo dnf -y remove "cvmfs*" || true


# install CernVM-FS RPM packages
echo "installing RPM packages... "
install_rpm "$CONFIG_PACKAGES"
install_rpm $LIBS_PACKAGE
install_rpm $FUSE3_PACKAGE
install_rpm $CLIENT_PACKAGE
install_rpm $SERVER_PACKAGE
install_rpm $DEVEL_PACKAGE

#NOTE: the yubikey machine is now setup to run tests on bare metal and does not need setup. 
# This is the setup used to initially setup the machine:

# CernVM-FS server needs 'jq' from epel
#echo "enabling epel yum repository..."
#install_from_repo epel-release

# setup environment
#echo -n "setting up CernVM-FS environment..."
#sudo cvmfs_config setup                          || die "fail (cvmfs_config setup)"
#sudo mkdir -p /var/log/cvmfs-test                || die "fail (mkdir /var/log/cvmfs-test)"
#sudo chown sftnight:sftnight /var/log/cvmfs-test || die "fail (chown /var/log/cvmfs-test)"
#sudo systemctl start autofs                      || die "fail (systemctl start autofs)"
#sudo cvmfs_config chksetup > /dev/null           || die "fail (cvmfs_config chksetup)"
#echo "done"
#
## install yubikey-related software
#install_from_repo opensc
#install_from_repo yubico-piv-tool
#sudo systemctl enable pcscd
#sudo systemctl start pcscd
