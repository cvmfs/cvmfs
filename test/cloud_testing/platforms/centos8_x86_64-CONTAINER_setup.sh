#!/bin/sh

# source the common platform independent functionality and option parsing
script_location=$(dirname $(readlink --canonicalize $0))
. ${script_location}/common_setup.sh

# install CernVM-FS RPM packages
echo "installing RPM packages... "
install_rpm "$CONFIG_PACKAGES"
install_rpm $CLIENT_PACKAGE

# Singularity is in epel
echo "enabling epel yum repository..."
install_from_repo epel-release    || die "fail (install epel-release)"

# Container runtimes and tools
install_from_repo singularity     || die "fail (install singularity)"
install_from_repo runc            || die "fail (install runc)"
install_from_repo fuse-overlayfs  || die "fail (install fuse-overlayfs)"
install_from_repo podman          || die "fail (install podman)"

sudo dnf config-manager --add-repo=https://download.docker.com/linux/centos/docker-ce.repo
sudo dnf install docker-ce --nobest -y || die "fail (install docker-ce)"
sudo systemctl start docker            || die "fail (starting docker)"
sudo usermod -aG docker sftnight
newgrp docker
docker ps                              || die "fail (accessing docker)"

install_rpm https://ecsft.cern.ch/cvmfs/dist/builddeps/minikube-1.9.2-0.x86_64.rpm || die "fail (install minikube)"

# setup environment
echo -n "setting up CernVM-FS environment..."
sudo cvmfs_config setup                          || die "fail (cvmfs_config setup)"
sudo mkdir -p /var/log/cvmfs-test                || die "fail (mkdir /var/log/cvmfs-test)"
sudo chown sftnight:sftnight /var/log/cvmfs-test || die "fail (chown /var/log/cvmfs-test)"
sudo systemctl start autofs                      || die "fail (systemctl start autofs)"
sudo cvmfs_config chksetup > /dev/null           || die "fail (cvmfs_config chksetup)"
echo "done"

# install docker
sudo yum install -y yum-utils
sudo yum-config-manager --add-repo https://download.docker.com/linux/centos/docker-ce.repo
# Docker is not yet supported on RHEL8 (firewalld issues). Need to do a workaround.
sudo yum install -y --nobest docker-ce
sudo firewall-cmd --zone=public --add-masquerade --permanent
sudo firewall-cmd --reload
sudo systemctl restart docker

disable_systemd_rate_limit
