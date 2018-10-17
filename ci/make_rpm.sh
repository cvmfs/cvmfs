#!/bin/sh

set -e

TARBALL=$1
BUILD_LOCATION=$2
PLATFORM=$3
VERSION=$4
RELEASE=$5

echo "Tarball: $TARBALL"
echo "Build location: $BUILD_LOCATION"
echo "Platform: $PLATFORM"
echo "Package version: $VERSION"
echo "Release: $RELEASE"

SCRIPT_LOCATION=$(cd "$(dirname "$0")"; pwd)
echo "Script location: $SCRIPT_LOCATION"

# Create togo project
echo "Creating togo project"
cd $BUILD_LOCATION
mkdir -p togo
cd togo
togo project create cvmfs-gateway

echo "Creating unpacking package tarball into togo project"
mkdir -p cvmfs-gateway/root/usr/libexec/cvmfs-gateway
cd cvmfs-gateway/root/usr/libexec/cvmfs-gateway
tar xf $BUILD_LOCATION/$TARBALL
cd $BUILD_LOCATION/togo/cvmfs-gateway
togo file exclude root/usr/libexec

# Place and flag config files in the togo workspace

# systemlog configuration
mkdir -p root/etc/{rsyslog.d,logrotate.d}
togo file exclude root/etc/rsyslog.d
togo file exclude root/etc/logrotate.d
cp -v root/usr/libexec/cvmfs-gateway/scripts/90-cvmfs-gateway.conf \
      root/etc/rsyslog.d
togo file flag config-nr root/etc/rsyslog.d/90-cvmfs-gateway.conf

# CentOS 7 uses systemd
if [ "x$PLATFORM" = "xcc7" ]; then
    mkdir -p root/etc/systemd/system
    togo file exclude root/etc/systemd/system
    cp -v root/usr/libexec/cvmfs-gateway/scripts/cvmfs-gateway.service \
          root/etc/systemd/system/
    togo file flag config-nr root/etc/systemd/system/cvmfs-gateway.service

    cp -v root/usr/libexec/cvmfs-gateway/scripts/90-cvmfs-gateway-rotate-systemd \
        root/etc/logrotate.d
    togo file flag config-nr root/etc/logrotate.d/90-cvmfs-gateway-rotate-systemd
else
    cp -v root/usr/libexec/cvmfs-gateway/scripts/90-cvmfs-gateway-rotate \
        root/etc/logrotate.d
    togo file flag config-nr root/etc/logrotate.d/90-cvmfs-gateway-rotate
fi

# cvmfs-gateway configuration files
mkdir -p root/etc/cvmfs/gateway
togo file exclude root/etc
togo file exclude root/etc/cvmfs
cp -v root/usr/libexec/cvmfs-gateway/etc/repo.json root/etc/cvmfs/gateway/
cp -v root/usr/libexec/cvmfs-gateway/etc/user.json root/etc/cvmfs/gateway/
togo file flag config-nr root/etc/cvmfs/gateway/repo.json
togo file flag config-nr root/etc/cvmfs/gateway/user.json

# Mnesia db location
mkdir -p root/var/lib/cvmfs-gateway
togo file exclude root/var/lib

# Copy spec file fragments into place
echo "Copying RPM spec file fragments"
cp -v $SCRIPT_LOCATION/spec/* ./spec/

# Replace template values in spec file header
echo "Configuring RPM spec file header"
sed -i -e "s/<<CVMFS_GATEWAY_VERSION>>/$VERSION/g" $BUILD_LOCATION/togo/cvmfs-gateway/spec/header
sed -i -e "s/<<CVMFS_GATEWAY_RELEASE>>/$RELEASE/g" $BUILD_LOCATION/togo/cvmfs-gateway/spec/header

# Build package
echo "Building RPM package"
togo build package

# Copy RPM and SRPM into place
echo "Copying RPMs to output location"
mkdir -p $BUILD_LOCATION/RPMS
cp -v ./rpms/*.rpm $BUILD_LOCATION/RPMS
cp -v ./rpms/src/*.rpm $BUILD_LOCATION/RPMS

# Create pkgmap
echo "Creating package map"
mkdir -p ${BUILD_LOCATION}/pkgmap
PKGMAP_FILE=${BUILD_LOCATION}/pkgmap/pkgmap.${PLATFORM}_x86_64
PACKAGE_NAME=cvmfs-gateway-${VERSION}-${RELEASE}$(rpm --eval "%{?dist}").x86_64.rpm
echo "[${BUILD_PLATFORM}_x86_64]" >> ${PKGMAP_FILE}
echo "gateway=${PACKAGE_NAME}" >> ${PKGMAP_FILE}

# Cleanup
cd $BUILD_LOCATION
rm -rf togo
