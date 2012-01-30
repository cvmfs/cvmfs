#!/bin/sh

CARES_VERSION=1.7.5
CURL_VERSION=7.24.0
FUSE_VERSION=2.8.4
FUSE4X_VERSION=2.8.5
ZLIB_VERSION=1.2.5
SPARSEHASH_VERSION=1.11

# put the extracted stuff out of source for compilation (location given by cmake)
outOfSource=$1

# check if bootstrapping already happened
if [ -f "$outOfSource/.decompressionDone" ]; then
	exit 0
fi

# C-ARES
cd externals/c-ares
tar xvfz c-ares-${CARES_VERSION}.tar.gz
mkdir -p "$outOfSource/c-ares/src"
mv c-ares-1.7.5/* "$outOfSource/c-ares/src"
cp src/* "$outOfSource/c-ares/src"
rm -rf c-ares-${CARES_VERSION}
cd ../..

# CURL
cd externals/libcurl
tar xfz curl-${CURL_VERSION}.tar.gz 
mkdir -p "$outOfSource/libcurl/src"
mv curl-${CURL_VERSION}/* "$outOfSource/libcurl/src"
cp src/* "$outOfSource/libcurl/src"
rm -rf curl-${CURL_VERSION}
cd ../..

# FUSE
cd externals/libfuse
tar xfz fuse-${FUSE_VERSION}.tar.gz
mkdir -p "$outOfSource/libfuse/src"
mv fuse-${FUSE_VERSION}/* "$outOfSource/libfuse/src"
patch -d "$outOfSource/libfuse" -N -p0 < fuse-drainout.patch
cp src/* "$outOfSource/libfuse/src"
rm -rf fuse-${FUSE_VERSION}
cd ../..

# Fuse4x
cd externals/libfuse4x
tar xfz fuse4x-${FUSE4X_VERSION}.tar.gz
mkdir -p "$outOfSource/libfuse4x/src"
mv fuse4x-${FUSE4X_VERSION}/* "$outOfSource/libfuse4x/src"
patch -d "$outOfSource/libfuse4x" -N -p0 < fuse4x-drainout.patch
cp src/* "$outOfSource/libfuse4x/src"
rm -rf fuse4x-${FUSE4X_VERSION}
cd ../..

# Zlib
cd externals/zlib
tar xfz zlib-${ZLIB_VERSION}.tar.gz
mkdir -p "$outOfSource/zlib/src"
mv zlib-${ZLIB_VERSION}/* "$outOfSource/zlib/src"
cp src/* "$outOfSource/zlib/src"
rm -rf zlib-${ZLIB_VERSION}
cd ../..

# sqlite3
cd externals/sqlite3
mkdir -p "$outOfSource/sqlite3/src"
cp src/* "$outOfSource/sqlite3/src"
cd ../..

# google sparse hash
cd externals/sparsehash
tar xfz sparsehash-${SPARSEHASH_VERSION}.tar.gz 
mkdir -p "$outOfSource/sparsehash/src"
mv sparsehash-${SPARSEHASH_VERSION}/* "$outOfSource/sparsehash/src"
cp src/* "$outOfSource/sparsehash/src"
rm -rf sparsehash-${SPARSEHASH_VERSION}
cd ../..

# create a hint that bootstrapping is already done
touch "$outOfSource/.decompressionDone"
