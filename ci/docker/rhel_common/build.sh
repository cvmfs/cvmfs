#!/bin/bash

# This is a unified script supposed to bootstrap RHEL-based chroot tarballs. It
# should _not_ be invoked by itself but instead sourced by platform dependent
# build scripts definding _all_ of the following variables:
#
#   SYSTEM_NAME    generic name of the OS to be built (i.e. slc5)
#   BASE_ARCH      CPU architecture to be built for (i386 or x86_64)
#   REPO_BASE_URL  URL to the base yum repository to be used for bootstrapping
#   GPG_KEY_PATHS  URL to the GPG key to be used for RPM signature validation
#   BASE_PACKAGES  list of packages to be installed for bootstrapping the system
#
# Additionally the following variables are optional:
#
#   PACKAGE_MGR    the package manager to use (defaults to 'yum')

set -e

[ ! -z SYSTEM_NAME   ] || die "SYSTEM_NAME is not defined"
[ ! -z BASE_ARCH     ] || die "BASE_ARCH is not defined"
[ ! -z REPO_BASE_URL ] || die "REPO_BASE_URL is not defined"
[ ! -z GPG_KEY_PATHS ] || die "GPG_KEY_PATHS is not defined"
[ ! -z BASE_PACKAGES ] || die "BASE_PACKAGES is not defined"

TARBALL_NAME="${SYSTEM_NAME}_${BASE_ARCH}.tar.gz"
DESTINATION="$(mktemp -d)"
YUM_REPO_CFG=/etc/yum/repos.d/${SYSTEM_NAME}_${BASE_ARCH}-bootstrap.repo
YUM_REPO_NAME=${SYSTEM_NAME}-${BASE_ARCH}-os-bootstrap
PACKAGE_MGR=${PACKAGE_MGR:=yum}

echo "installing cleanup handler..."
cleanup() {
  echo "cleaning up the build environment..."
  umount ${DESTINATION}/dev  || true
  umount ${DESTINATION}/proc || true
  umount ${DESTINATION}/sys  || true
  rm -f $YUM_REPO_CFG        || true
  rm -fR $DESTINATION        || true
}
trap cleanup EXIT HUP INT TERM

echo "checking yum installation..."
install_yum_repo_keys
check_yum_environment

echo "setting up bootstrap repository..."
cat > $YUM_REPO_CFG << EOF
[$YUM_REPO_NAME]
name=$SYSTEM_NAME base system packages
baseurl=$REPO_BASE_URL
gpgkey=$GPG_KEY_PATHS
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
    $BASE_PACKAGES

mtab="${DESTINATION}/etc/mtab"
[ -e $mtab ] || [ -h $mtab ] || touch $mtab

echo "fixing yum configuration files to architecture..."
for f in $(find ${DESTINATION}/etc/yum.repos.d -type f); do
  echo -n "patching ${f}... "
  sed -i -e "s/\$basearch/$BASE_ARCH/g" $f
  echo "done"
done

echo "do generic system setup..."
cp /etc/resolv.conf ${DESTINATION}/etc/resolv.conf
echo "NETWORKING=yes" > ${DESTINATION}/etc/sysconfig/network
chroot $DESTINATION ln -f /usr/share/zoneinfo/Etc/UTC /etc/localtime
mkdir -p ${DESTINATION}/dev ${DESTINATION}/sys ${DESTINATION}/proc
mount --bind /dev ${DESTINATION}/dev
mount -t sysfs sys ${DESTINATION}/sys/

echo "recreating RPM database with chroot'ed RPM version..."
rm -fR $rpm_db_dir && mkdir -p $rpm_db_dir
chroot $DESTINATION /bin/rpm --initdb
chroot $DESTINATION /bin/rpm -ivh --justdb '/var/cache/yum/*/packages/*.rpm'
rm -r ${DESTINATION}/var/cache/yum/

echo "doing final housekeeping..."
chroot $DESTINATION $PACKAGE_MGR clean all
rm -f ${DESTINATION}/etc/resolv.conf
umount ${DESTINATION}/dev  || true # ignore failing umount
umount ${DESTINATION}/proc || true # ignore failing umount
umount ${DESTINATION}/sys  || true # ignore failing umount

echo "packaging up the image..."
tar -czf $TARBALL_NAME -C $DESTINATION .

echo "created $TARBALL_NAME ($(stat --format='%s' $TARBALL_NAME) bytes)"
