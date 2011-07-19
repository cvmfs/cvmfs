#!/bin/sh

CURL_VERSION=7.21.3
FUSE_VERSION=2.8.4
FUSE4X_VERSION=2.8.5
REDIRFS_VERSION=SVN-671
JEMALLOC_VERSION=2.2.1
ZLIB_VERSION=1.2.5

cd 3rdParty/libcurl
tar xfz curl-${CURL_VERSION}.tar.gz 
mv curl-${CURL_VERSION}/* src/
rm -rf curl-${CURL_VERSION}
cd ../..

cd 3rdParty/libfuse
tar xfz fuse-${FUSE_VERSION}.tar.gz
mv fuse-${FUSE_VERSION}/* src/
rm -rf fuse-${FUSE_VERSION}
patch -N -p0 < fuse-drainout.patch
cd ../..

cd 3rdParty/libfuse4x
tar xfz fuse4x-${FUSE4X_VERSION}.tar.gz
mv fuse4x-${FUSE4X_VERSION}/* src/
rm -rf fuse4x-${FUSE4X_VERSION}
patch -N -p0 < fuse4x-drainout.patch
cd ../..

cd kernel/redirfs
tar xfz redirfs-${REDIRFS_VERSION}.tar.gz
mv redirfs-${REDIRFS_VERSION}/* src/
rm -rf redirfs-${REDIRFS_VERSION}
cd ../..

cd 3rdParty/jemalloc
tar xfj jemalloc-${JEMALLOC_VERSION}.tar.bz2
mv jemalloc-${JEMALLOC_VERSION}/* src/
mv src/configure.ac src/configure.ac.vanilla
touch src/configure.ac
patch -N -p0 < jemalloc-2.2.1-64bit_literals.patch
rm -rf jemalloc-${JEMALLOC_VERSION} 
cd ../..

cd 3rdParty/zlib
tar xfz zlib-${ZLIB_VERSION}.tar.gz
mv zlib-${ZLIB_VERSION}/* src/
rm -rf zlib-${ZLIB_VERSION}
cd ../..