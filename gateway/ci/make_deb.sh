#!/bin/bash

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

if [ x"$PLATFORM" = xubuntu1604 ]; then
    PACKAGE_NAME_SUFFIX="+ubuntu16.04_amd64"
elif [ x"$PLATFORM" = xubuntu1804 ]; then
    PACKAGE_NAME_SUFFIX="+ubuntu18.04_amd64"
fi
PACKAGE_NAME=cvmfs-gateway_$VERSION~$RELEASE$PACKAGE_NAME_SUFFIX.deb

mkdir -p ${CVMFS_GATEWAY_SOURCES}/DEBS

if [ -e /etc/profile.d/rvm.sh ]; then
    . /etc/profile.d/rvm.sh
fi

WORKSPACE=${CVMFS_GATEWAY_SOURCES}/pkg_ws
mkdir -p $WORKSPACE

mkdir -p $WORKSPACE/etc/systemd/system
mkdir -p $WORKSPACE/etc/cvmfs/gateway
mkdir -p $WORKSPACE/usr/bin
mkdir -p $WORKSPACE/var/lib/cvmfs-gateway

cp -v ${CVMFS_GATEWAY_SOURCES}/gateway $WORKSPACE/usr/bin/cvmfs-gateway

cp -v ${CVMFS_GATEWAY_SOURCES}/pkg/cvmfs-gateway.service \
    $WORKSPACE/etc/systemd/system/
cp -v ${CVMFS_GATEWAY_SOURCES}/config/repo.json $WORKSPACE/etc/cvmfs/gateway/
cp -v ${CVMFS_GATEWAY_SOURCES}/config/user.json $WORKSPACE/etc/cvmfs/gateway/

pushd $WORKSPACE
fpm -s dir -t deb \
    --package ../DEBS/$PACKAGE_NAME \
    --version $VERSION \
    --name cvmfs-gateway \
    --maintainer "Radu Popescu <radu.popescu@cern.ch>" \
    --description "CernVM-FS Repository Gateway" \
    --url "http://cernvm.cern.ch" \
    --license "BSD-3-Clause" \
    --depends "cvmfs-server > 2.5.2" \
    --directories var/lib/cvmfs-gateway \
    --config-files etc/cvmfs/gateway/repo.json \
    --config-files etc/cvmfs/gateway/user.json \
    --config-files etc/systemd/system/cvmfs-gateway.service \
    --exclude etc/systemd/system \
    --no-deb-systemd-restart-after-upgrade \
    --after-install ${CVMFS_GATEWAY_SOURCES}/pkg/setup_deb.sh \
    --chdir $WORKSPACE \
    ./
popd

mkdir -p ${CVMFS_GATEWAY_SOURCES}/pkgmap
PKGMAP_FILE=${CVMFS_GATEWAY_SOURCES}/pkgmap/pkgmap.${PLATFORM}_x86_64
echo "[${PLATFORM}_x86_64]" >> $PKGMAP_FILE
echo "gateway=$PACKAGE_NAME" >> $PKGMAP_FILE

rm -rf $WORKSPACE
