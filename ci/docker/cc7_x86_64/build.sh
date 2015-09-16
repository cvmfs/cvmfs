#!/bin/bash

set -e

SCRIPT_LOCATION=$(cd "$(dirname "$0")"; pwd)
. ${SCRIPT_LOCATION}/../common.sh

BASE_ARCH="x86_64"

if [ $# -ne 1 ]; then
  echo "Builds a minimal CentOS CERN 7 $BASE_ARCH chroot tarball"
  echo "Usage: ./build.sh <tarball location>"
  exit 1
fi

IMAGE="$1"
DESTINATION="$(mktemp -d)"
YUM_REPO_CFG=/etc/yum/repos.d/cc7_bootstrap.repo
YUM_REPO_NAME=cc7-base-bootstrap

echo "checking yum installation..."
check_yum_environment

echo "setting up bootstrap repository..."
cat > $YUM_REPO_CFG << EOF
[$YUM_REPO_NAME]
name=CentOS-7 - Base
baseurl=http://linuxsoft.cern.ch/cern/centos/7/os/$BASE_ARCH/
gpgkey=file:///etc/pki/rpm-gpg/RPM-GPG-KEY-CentOS-7
gpgcheck=1
enabled=0
EOF

echo "creating chroot dir..."
[ ! -d $DESTINATION ] || rm -fR $DESTINATION
mkdir -p $DESTINATION

echo "initializing RPM database..."
rpm --root $DESTINATION --initdb

echo -n "looking for created RPM database... "
rpm_db_dir="$(find $DESTINATION -type d | tail -n1)"
echo $rpm_db_dir

echo "bootstrapping the system..."
yum --disablerepo='*'             \
    --enablerepo="$YUM_REPO_NAME" \
    --installroot=$DESTINATION    \
    -y install                    \
    centos-release coreutils tar iputils rpm yum
touch ${DESTINATION}/etc/mtab

echo "fixing yum configuration files to architecture..."
fix_yum_config_to_architecture ${DESTINATION}/etc/yum.repos.d $BASE_ARCH

echo "do generic system setup..."
setup_base_configuration $DESTINATION

echo "recreating RPM database with chroot'ed RPM version..."
recreate_rpm_database $DESTINATION $rpm_db_dir

echo "doing final housekeeping..."
chroot $DESTINATION yum clean all
rm -f ${DESTINATION}/etc/resolv.conf
umount ${DESTINATION}/dev

echo "packaging up the image..."
chroot $DESTINATION tar -czf - . > $IMAGE

echo "cleaning up the build environment..."
rm -f $YUM_REPO_CFG
rm -fR $DESTINATION

echo "created $IMAGE ($(stat --format='%s' $IMAGE) bytes)"
