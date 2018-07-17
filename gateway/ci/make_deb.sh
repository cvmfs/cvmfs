#!/bin/bash

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

if [ x"$PLATFORM" = xubuntu1604 ]; then
    PACKAGE_NAME_SUFFIX="+ubuntu16.04_amd64"
elif [ x"$PLATFORM" = xubuntu1804 ]; then
    PACKAGE_NAME_SUFFIX="+ubuntu18.04_amd64"
fi
PACKAGE_NAME=cvmfs-gateway_$VERSION~$RELEASE$PACKAGE_NAME_SUFFIX.deb

mkdir -p ${BUILD_LOCATION}/DEBS

if [ -e /etc/profile.d/rvm.sh ]; then
    . /etc/profile.d/rvm.sh
fi

cd $BUILD_LOCATION

fpm -s tar \
    -t deb \
    --prefix /usr/libexec/cvmfs-gateway \
    --package DEBS/$PACKAGE_NAME \
    --version $VERSION \
    --name cvmfs-gateway \
    --maintainer "Radu Popescu <radu.popescu@cern.ch>" \
    --description "CernVM-FS Repository Gateway" \
    --url "http://cernvm.cern.ch" \
    --license "BSD-3-Clause" \
    $BUILD_LOCATION/$TARBALL

mkdir -p $BUILD_LOCATION/pkgmap
PKGMAP_FILE=$BUILD_LOCATION/pkgmap/pkgmap.${PLATFORM}_x86_64
echo "[${PLATFORM}_x86_64]" >> $PKGMAP_FILE
echo "gateway=$PACKAGE_NAME" >> $PKGMAP_FILE
