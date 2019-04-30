#!/bin/sh

set -e

CVMFS_GATEWAY_SOURCES=$1
PLATFORM=$2
VERSION=$3
RELEASE=$4

PROJECT_NAME=cvmfs-gateway

echo "Location: $CVMFS_GATEWAY_SOURCES"
echo "Platform: $PLATFORM"
echo "Package version: $VERSION"
echo "Release: $RELEASE"

SCRIPT_LOCATION=$(cd "$(dirname "$0")"; pwd)
echo "Script location: $SCRIPT_LOCATION"

echo "Building package"
cd ${CVMFS_GATEWAY_SOURCES}
export GOPATH=${GOPATH:=${CVMFS_GATEWAY_SOURCES}/../go}
go build

echo "Creating togo project"
mkdir -p ${CVMFS_GATEWAY_SOURCES}/togo
cd $CVMFS_GATEWAY_SOURCES/togo
togo project create ${PROJECT_NAME}
TOGO_PROJECT=${CVMFS_GATEWAY_SOURCES}/togo/${PROJECT_NAME}
cd ${TOGO_PROJECT}

### Install executable to togo root
mkdir -p ${TOGO_PROJECT}/root/usr/bin
cp -v ${CVMFS_GATEWAY_SOURCES}/gateway ${TOGO_PROJECT}/root/usr/bin/cvmfs-gateway
togo file exclude root/usr/bin

# Place and flag config files in the togo workspace
if [ "x$PLATFORM" = "xcc7" ]; then
    mkdir -p ${TOGO_PROJECT}/root/etc/systemd/system
    togo file exclude root/etc/systemd/system
    cp -v ${CVMFS_GATEWAY_SOURCES}/pkg/cvmfs-gateway.service \
        ${TOGO_PROJECT}/root/etc/systemd/system/
    togo file flag config-nr root/etc/systemd/system/cvmfs-gateway.service
else
    mkdir -p ${TOGO_PROJECT}/root/etc/init.d
    togo file exclude root/etc/init.d
    cp -v ${CVMFS_GATEWAY_SOURCES}/pkg/cvmfs-gateway.initrd \
        ${TOGO_PROJECT}/root/etc/init.d/cvmfs-gateway
    togo file flag config-nr root/etc/init.d/cvmfs-gateway

    mkdir -p ${TOGO_PROJECT}/root/etc/logrotate.d
    togo file exclude root/etc/logrotate.d
    cp -v ${CVMFS_GATEWAY_SOURCES}/pkg/90-cvmfs-gateway-rotate \
        ${TOGO_PROJECT}/root/etc/logrotate.d
    togo file flag config-nr root/etc/logrotate.d/90-cvmfs-gateway-rotate
fi

# cvmfs-gateway configuration files
mkdir -p ${TOGO_PROJECT}/root/etc/cvmfs/gateway
togo file exclude root/etc
togo file exclude root/etc/cvmfs
cp -v ${CVMFS_GATEWAY_SOURCES}/config/repo.json ${TOGO_PROJECT}/root/etc/cvmfs/gateway/
cp -v ${CVMFS_GATEWAY_SOURCES}/config/user.json ${TOGO_PROJECT}/root/etc/cvmfs/gateway/
togo file flag config-nr root/etc/cvmfs/gateway/repo.json
togo file flag config-nr root/etc/cvmfs/gateway/user.json

# Mnesia db location
mkdir -p ${TOGO_PROJECT}/root/var/lib/cvmfs-gateway
togo file exclude root/var/lib

# Copy spec file fragments into place
echo "Copying RPM spec file fragments"
cp -v ${CVMFS_GATEWAY_SOURCES}/pkg/spec/* ./spec/

# Replace template values in spec file header
echo "Configuring RPM spec file header"
sed -i -e "s/<<CVMFS_GATEWAY_VERSION>>/$VERSION/g" $CVMFS_GATEWAY_SOURCES/togo/cvmfs-gateway/spec/header
sed -i -e "s/<<CVMFS_GATEWAY_RELEASE>>/$RELEASE/g" $CVMFS_GATEWAY_SOURCES/togo/cvmfs-gateway/spec/header

# Build package
echo "Building RPM package"
togo build package

# Copy RPM and SRPM into place
echo "Copying RPMs to output location"
mkdir -p $CVMFS_GATEWAY_SOURCES/RPMS
cp -v ./rpms/*.rpm $CVMFS_GATEWAY_SOURCES/RPMS
cp -v ./rpms/src/*.rpm $CVMFS_GATEWAY_SOURCES/RPMS

# Create pkgmap
echo "Creating package map"
mkdir -p ${CVMFS_GATEWAY_SOURCES}/pkgmap
PKGMAP_FILE=${CVMFS_GATEWAY_SOURCES}/pkgmap/pkgmap.${PLATFORM}_x86_64
PACKAGE_NAME=cvmfs-gateway-${VERSION}-${RELEASE}$(rpm --eval "%{?dist}").x86_64.rpm
echo "[${BUILD_PLATFORM}_x86_64]" >> ${PKGMAP_FILE}
echo "gateway=${PACKAGE_NAME}" >> ${PKGMAP_FILE}

# Cleanup
cd $CVMFS_GATEWAY_SOURCES
rm -rf togo
