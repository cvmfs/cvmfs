#!/bin/sh

# source the common platform independent functionality and option parsing
script_location=$(dirname $(readlink --canonicalize $0))
. ${script_location}/common_setup.sh

# install CernVM-FS RPM packages
echo "installing RPM packages... "
install_rpm "$CONFIG_PACKAGES"
install_rpm $LIBS_PACKAGE
install_rpm $CLIENT_PACKAGE
install_rpm $FUSE3_PACKAGE

[ "x$SERVICE_CONTAINER" != "x" ] || die "fail (service container missing)"
mkdir -p /tmp/cvmfs-service-container
cp -v $SERVICE_CONTAINER /tmp/cvmfs-service-container/docker.tar.gz || \
  die "fail (planting service container)"

# Singularity is in epel
echo "enabling epel yum repository..."
install_from_repo epel-release    || die "fail (install epel-release)"

# Container runtimes and tools
install_from_repo apptainer       || die "fail (install singularity)"
install_from_repo runc            || die "fail (install runc)"
install_from_repo fuse-overlayfs  || die "fail (install fuse-overlayfs)"
install_from_repo podman          || die "fail (install podman)"
install_from_repo jq              || die "fail (install jq)"

# Now it gets hacky: podman and containerd.io conflict because the Docker
# containerd.io package insists on bringing its own runc binary.
# We install containerd manually and skip its runc but keep the one from RH.
# Furthermore, we use the docker-ce-test repository to get containerd > 1.4
# so that we can test the remote snapshotter
sudo dnf config-manager --add-repo=https://download.docker.com/linux/centos/docker-ce.repo

yumdownloader --enablerepo=docker-ce-test containerd.io
rpm2cpio containerd.io-*.rpm | cpio -id
sudo cp -av etc/containerd /etc
sudo cp -av usr/bin/containerd* usr/bin/ctr /usr/bin/
sudo cp usr/lib/systemd/system/containerd.service /usr/lib/systemd/system/
# Don't bother with SELinux
sudo setenforce 0
sudo systemctl daemon-reload
sudo systemctl enable containerd
sudo systemctl start containerd
systemctl status containerd  || die "fail (setup containerd.io)"
# Now: satisfy the RPM dependency
sudo rpm -i --justdb --replacefiles --nodeps containerd.io*.rpm

sudo dnf install docker-ce -y || die "fail (install docker-ce)"
sudo firewall-cmd --zone=public --add-masquerade --permanent
sudo firewall-cmd --reload

# Make docker available to sftnight
sudo mkdir /usr/lib/systemd/system/docker.socket.d
echo "[Socket]" | sudo tee /usr/lib/systemd/system/docker.socket.d/10-sftnight.conf
echo "SocketGroup=sftnight" | sudo tee -a /usr/lib/systemd/system/docker.socket.d/10-sftnight.conf
sudo systemctl daemon-reload

sudo systemctl start docker            || die "fail (starting docker)"
docker ps                              || die "fail (accessing docker)"

install_rpm https://ecsft.cern.ch/dist/cvmfs/builddeps/minikube-1.9.2-0.x86_64.rpm || die "fail (install minikube)"

sudo curl -o /usr/bin/kind \
  https://ecsft.cern.ch/dist/cvmfs/builddeps/kind-v0.8.1 || die "fail (download kind)"
sudo chmod +x /usr/bin/kind                              || die "fail (activate kind)"
kind version                                             || die "fail (install kind)"

# install packages to deploy CSI driver
install_from_repo git     || die "fail (install git)"
install_from_repo golang  || die "fail (install golang)"

# setup environment
echo -n "setting up CernVM-FS environment..."
sudo cvmfs_config setup                          || die "fail (cvmfs_config setup)"
sudo mkdir -p /var/log/cvmfs-test                || die "fail (mkdir /var/log/cvmfs-test)"
sudo chown sftnight:sftnight /var/log/cvmfs-test || die "fail (chown /var/log/cvmfs-test)"
sudo systemctl start autofs                      || die "fail (systemctl start autofs)"
sudo cvmfs_config chksetup > /dev/null           || die "fail (cvmfs_config chksetup)"
echo "done"

disable_systemd_rate_limit
