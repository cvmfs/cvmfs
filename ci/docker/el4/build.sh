#!/bin/bash

## execute in a Docker container to build the tarball
##   docker run --privileged -i -t -v $PWD:/srv centos:centos6 /srv/build.sh

# close stdin
exec 0<&-

set -e -u -x

DEST_IMG="/srv/slc49_i386.tar.gz"

rm -f ${DEST_IMG}

instroot=$(mktemp -d)
tmpyum=$(mktemp)

cat << EOF >  ${tmpyum}
[main]
debuglevel=2
exactarch=1
obsoletes=1
gpgcheck=1
plugins=1
installonly_limit=5
distroverpkg=centos-release

[sl4-base]
name=SL-4 - Base
baseurl=http://cvm-storage00.cern.ch/yum/sl4/os/i386/RPMS/
gpgcheck=0

[sl4-fastbugs]
name=SL-4 - Fastbugs
baseurl=http://cvm-storage00.cern.ch/yum/sl4/fastbugs/i386/
gpgcheck=0

[sl4-epel]
name=SL-4 - EPEL
baseurl=http://cvm-storage00.cern.ch/yum/sl4/epel/i386/
gpgcheck=0
EOF

## touch, chmod; /dev/null; /etc/fstab;
mkdir ${instroot}/{dev,etc,proc}
mknod ${instroot}/dev/null c 1 3
touch ${instroot}/etc/fstab

yum \
    -c ${tmpyum} \
    --disablerepo='*' \
    --enablerepo='sl4-*' \
    --setopt=cachedir=${instroot}/var/cache/yum \
    --setopt=logfile=${instroot}/var/log/yum.log \
    --setopt=keepcache=1 \
    --setopt=diskspacecheck=0 \
    -y \
    --installroot=${instroot} \
    install \
    sl-release coreutils iputils which curl rpm yum yum-conf || echo "ignoring failed yum; $?"

cp /etc/resolv.conf ${instroot}/etc/resolv.conf

## yum/rpm on centos6 creates databases that can't be read by centos4's yum
## http://lists.centos.org/pipermail/centos/2012-December/130752.html
rm ${instroot}/var/lib/rpm/*
chroot ${instroot} /bin/rpm --initdb
chroot ${instroot} /bin/rpm -ivh --justdb '/var/cache/yum/*/packages/*.rpm'
rm -r ${instroot}/var/cache/yum/

chroot ${instroot} sh -c 'echo "NETWORKING=yes" > /etc/sysconfig/network'

## set timezone of container to UTC
chroot ${instroot} ln -f /usr/share/zoneinfo/Etc/UTC /etc/localtime

## reset yum repositories
rm -f ${instroot}/etc/yum.repos.d/sl-rhaps.repo      \
      ${instroot}/etc/yum.repos.d/sl-testing.repo    \
      ${instroot}/etc/yum.repos.d/sl4x-contrib.repo  \
      ${instroot}/etc/yum.repos.d/sl4x-fastbugs.repo \
      ${instroot}/etc/yum.repos.d/sl4x-errata.repo   \
      ${instroot}/etc/yum.repos.d/dag.repo           \
      ${instroot}/etc/yum.repos.d/dries.repo         \
      ${instroot}/etc/yum.repos.d/atrpms.repo        \
      ${instroot}/etc/yum.repos.d/sl4x.repo

cat << EOF > ${instroot}/etc/yum.repos.d/sl4x.repo
[sl-base]
name=SL 4 base
baseurl=http://cvm-storage00.cern.ch/yum/sl4/os/i386/RPMS/
enabled=1
gpgcheck=0
gpgkey=file:///etc/pki/rpm-gpg/RPM-GPG-KEY-csieh file:///etc/pki/rpm-gpg/RPM-GPG-KEY-dawson file:///etc/pki/rpm-gpg/RPM-GPG-KEY-jpolok file:///etc/pki/rpm-gpg/RPM-GPG-KEY-cern file:///etc/pki/rpm-gpg/RPM-GPG-KEY-sl file:///etc/pki/rpm-gpg/RPM-GPG-KEY-sl4
EOF

cat << EOF > ${instroot}/etc/yum.repos.d/sl4x-fastbugs.repo
[sl-fastbug]
name=SL 4 base
baseurl=http://cvm-storage00.cern.ch/yum/sl4/fastbugs/i386/
enabled=1
gpgcheck=0
gpgkey=file:///etc/pki/rpm-gpg/RPM-GPG-KEY-csieh file:///etc/pki/rpm-gpg/RPM-GPG-KEY-dawson file:///etc/pki/rpm-gpg/RPM-GPG-KEY-jpolok file:///etc/pki/rpm-gpg/RPM-GPG-KEY-cern file:///etc/pki/rpm-gpg/RPM-GPG-KEY-sl file:///etc/pki/rpm-gpg/RPM-GPG-KEY-sl4
EOF

cat << EOF > ${instroot}/etc/yum.repos.d/sl4x-epel.repo
[sl-epel]
name=SL 4 EPEL
baseurl=http://cvm-storage00.cern.ch/yum/sl4/epel/i386/
enabled=1
gpgcheck=0
EOF


chroot ${instroot} yum clean all

## clean up mounts ($instroot/proc mounted by yum, apparently)
umount ${instroot}/proc
rm -f ${instroot}/etc/resolv.conf

## xz gives the smallest size by far, compared to bzip2 and gzip, by like 50%!
## … but somewhere along the line Docker stopped supporting it.
chroot ${instroot} tar -czf - . > ${DEST_IMG}
