#!/bin/bash

## execute in a Docker container to build the tarball
##   docker run --privileged -i -t -v $PWD:/srv centos:centos6 /srv/build.sh

# close stdin
exec 0<&-

set -e -u -x

DEST_IMG="/srv/centos49.tar.gz"

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

[c4-base]
name=CentOS-4 - Base
baseurl=http://vault.centos.org/4.9/os/x86_64/
gpgcheck=1
gpgkey=http://vault.centos.org/RPM-GPG-KEY-CentOS-4

[c4-updates]
name=CentOS-4 - Updates
baseurl=http://vault.centos.org/4.9/updates/x86_64/
gpgcheck=1
gpgkey=http://vault.centos.org/RPM-GPG-KEY-CentOS-4
EOF

## touch, chmod; /dev/null; /etc/fstab; 
mkdir ${instroot}/{dev,etc,proc}
mknod ${instroot}/dev/null c 1 3 
touch ${instroot}/etc/fstab

yum \
    -c ${tmpyum} \
    --disablerepo='*' \
    --enablerepo='c4-*' \
    --setopt=cachedir=${instroot}/var/cache/yum \
    --setopt=logfile=${instroot}/var/log/yum.log \
    --setopt=keepcache=1 \
    --setopt=diskspacecheck=0 \
    -y \
    --installroot=${instroot} \
    install \
    centos-release yum iputils coreutils which curl || echo "ignoring failed yum; $?"

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

sed -i \
    -e '/^mirrorlist/d' \
    -e 's@^#baseurl=http://mirror.centos.org/centos/$releasever/@baseurl=http://vault.centos.org/4.9/@g' \
    ${instroot}/etc/yum.repos.d/CentOS*.repo

## epel
curl -f -L -o ${instroot}/tmp/RPM-GPG-KEY-EPEL-4 http://dl.fedoraproject.org/pub/epel/RPM-GPG-KEY-EPEL-4
curl -f -L -o ${instroot}/tmp/epel-release-4-10.noarch.rpm https://dl.fedoraproject.org/pub/epel/4/x86_64/epel-release-4-10.noarch.rpm
chroot ${instroot} rpm --import /tmp/RPM-GPG-KEY-EPEL-4
chroot ${instroot} yum localinstall -y /tmp/epel-release-4-10.noarch.rpm
rm -f ${instroot}/tmp/epel-release-4-10.noarch.rpm ${instroot}/tmp/RPM-GPG-KEY-EPEL-4

chroot ${instroot} yum clean all

## clean up mounts ($instroot/proc mounted by yum, apparently)
umount ${instroot}/proc
rm -f ${instroot}/etc/resolv.conf

## xz gives the smallest size by far, compared to bzip2 and gzip, by like 50%!
## … but somewhere along the line Docker stopped supporting it.
chroot ${instroot} tar -czf - . > ${DEST_IMG}
