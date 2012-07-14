#!/bin/sh

CARES_VERSION=1.7.5
CURL_VERSION=7.24.0
ZLIB_VERSION=1.2.5
SPARSEHASH_VERSION=1.11
LEVELDB_VERSION=1.5.0

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

# Murmur
cd externals/murmur
mkdir -p "$outOfSource/murmur/src"
cp src/* "$outOfSource/murmur/src"
cd ../..

# google sparse hash
cd externals/sparsehash
tar xfz sparsehash-${SPARSEHASH_VERSION}.tar.gz 
mkdir -p "$outOfSource/sparsehash/src"
mv sparsehash-${SPARSEHASH_VERSION}/* "$outOfSource/sparsehash/src"
cp src/* "$outOfSource/sparsehash/src"
rm -rf sparsehash-${SPARSEHASH_VERSION}
cd ../..

# leveldb
cd externals/leveldb
tar xvfz leveldb-${LEVELDB_VERSION}.tar.gz
mkdir -p "$outOfSource/leveldb/src"
mv leveldb-${LEVELDB_VERSION}/* "$outOfSource/leveldb/src"
cp src/* "$outOfSource/leveldb/src"
rm -rf leveldb-${LEVELDB_VERSION}
cd ../..

# create a hint that bootstrapping is already done
touch "$outOfSource/.decompressionDone"
