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
togo configure -n "Radu Popescu" -e "radu.popescu@cern.ch"
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
